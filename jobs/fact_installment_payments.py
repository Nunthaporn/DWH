from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ✅ Load env
load_dotenv()

# ✅ DB connections
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)
task_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task"
)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_installment_data():
    df_plan = pd.read_sql("""
        SELECT quo_num
        FROM fin_system_select_plan
        WHERE datestart >= '2025-01-01' AND datestart < '2025-07-01'
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """, source_engine)

    df_installment = pd.read_sql("""
        SELECT quo_num, money_one, money_two, money_three, money_four,
               money_five, money_six, money_seven, money_eight, money_nine,
               money_ten, money_eleven, money_twelve,
               date_one, date_two, date_three, date_four, date_five,
               date_six, date_seven, date_eight, date_nine, date_ten,
               date_eleven, date_twelve, numpay
        FROM fin_installment
    """, source_engine)

    df_order = pd.read_sql("""
        SELECT quo_num, order_number
        FROM fin_order
        WHERE type_insure IN ('ประกันรถ', 'ตรอ')
    """, task_engine)

    df_finance = pd.read_sql("""
        SELECT order_number, datepay_one, datepay_two, datepay_three, datepay_four,
               datepay_five, datepay_six, datepay_seven, datepay_eight,
               datepay_nine, datepay_ten, datepay_eleven, datepay_twelve,
               moneypay_one, moneypay_two, moneypay_three, moneypay_four,
               moneypay_five, moneypay_six, moneypay_seven, moneypay_eight,
               moneypay_nine, moneypay_ten, moneypay_eleven, moneypay_twelve,
               numpay
        FROM fin_finance
        WHERE order_number REGEXP '[A-Z]+25[0-9]{2}-[0-9]+'
    """, task_engine)

    df_bill = pd.read_sql("""
        SELECT order_number, bill_receipt, bill_receipt2, bill_receipt3,
               bill_receipt4, bill_receipt5, bill_receipt6, bill_receipt7,
               bill_receipt8, bill_receipt9, bill_receipt10, bill_receipt11, bill_receipt12
        FROM fin_bill
    """, task_engine)

    df_late_fee = pd.read_sql("""
        SELECT orderNumber, penaltyPay, numPay
        FROM FIN_Account_AttachSlip_PathImageSlip
        WHERE checkPay IN ('ค่าปรับ', 'ค่างวด/ค่าปรับ')
    """, task_engine)

    df_test = pd.read_sql("""
        SELECT quo_num
        FROM fin_system_select_plan
        WHERE name IN ('ทดสอบ','test')
          AND type_insure IN ('ประกันรถ', 'ตรอ')
    """, source_engine)

    return df_plan, df_installment, df_order, df_finance, df_bill, df_late_fee, df_test

@op
def clean_installment_data(inputs):
    df_plan, df_inst, df_order, df_fin, df_bill, df_fee, df_test = inputs

    # 1. เตรียมข้อมูลค่างวด + วันที่
    df_inst['numpay'] = pd.to_numeric(df_inst['numpay'], errors='coerce')
    df_filtered = df_inst[df_inst['numpay'].notna() & (df_inst['numpay'] > 0)]

    money_cols = [f'money_{n}' for n in ['one', 'two', 'three', 'four', 'five', 'six',
                                         'seven', 'eight', 'nine', 'ten', 'eleven', 'twelve']]
    date_cols = [f'date_{n}' for n in ['one', 'two', 'three', 'four', 'five', 'six',
                                       'seven', 'eight', 'nine', 'ten', 'eleven', 'twelve']]

    df_money = df_filtered.melt(id_vars=['quo_num', 'numpay'], value_vars=money_cols,
                                 var_name='installment_period', value_name='installment_amount')
    df_date = df_filtered.melt(id_vars=['quo_num', 'numpay'], value_vars=date_cols,
                                var_name='due_date_period', value_name='due_date')

    df_combined = pd.concat([df_money.reset_index(drop=True), df_date['due_date']], axis=1)
    df_combined['installment_number'] = df_combined.groupby('quo_num').cumcount() + 1
    df_combined = df_combined[df_combined['installment_number'] <= df_combined['numpay']]
    df_combined = df_combined.sort_values(by=['quo_num', 'due_date'])
    df_combined['installment_number'] = df_combined.groupby('quo_num').cumcount() + 1

    # 2. ผูก order_number
    df_join = pd.merge(df_combined, df_order, on='quo_num', how='left')

    # 3. เตรียมข้อมูลการชำระ
    num_to_name = ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight',
                   'nine', 'ten', 'eleven', 'twelve']
    rows = []

    df_fin['numpay'] = pd.to_numeric(df_fin['numpay'], errors='coerce')
    df_fin = df_fin[df_fin['numpay'].notna() & (df_fin['numpay'] > 0)]

    for _, row in df_fin.iterrows():
        for i, sfx in enumerate(num_to_name, start=1):
            money = row.get(f'moneypay_{sfx}')
            raw_date = str(row.get(f'datepay_{sfx}'))
            match = pd.Series(raw_date).str.extract(r'(\d{4}-\d{1,2}-\d{1,2})')[0]
            clean_date = pd.to_datetime(match.values[0], errors='coerce') if pd.notna(match.values[0]) else None
            if pd.notna(clean_date) and clean_date.year == 2026:
                clean_date = clean_date.replace(year=2025)
            rows.append({
                'order_number': row['order_number'],
                'payment_amount': money,
                'payment_date': clean_date,
                'installment_number': i
            })

    df_payment = pd.DataFrame(rows)

    # 4. เตรียม payment proof
    df_proof = df_bill.melt(id_vars=['order_number'],
        value_vars=[f'bill_receipt{i if i > 1 else ""}' for i in range(1, 13)],
        value_name='payment_proof')
    df_proof = df_proof[df_proof['payment_proof'].notna()].reset_index()
    df_proof['installment_number'] = df_proof.groupby('order_number').cumcount() + 1
    df_proof = df_proof[['order_number', 'installment_number', 'payment_proof']]

    # 5. รวมทั้งหมด
    df = pd.merge(df_join, df_payment, on=['order_number', 'installment_number'], how='left')
    df = pd.merge(df, df_proof, on=['order_number', 'installment_number'], how='left')

    df.loc[
        (df['installment_number'] == 1) & (df['payment_amount'].isna()),
        'payment_amount'
    ] = df['installment_amount']

    # 6. เพิ่ม late_fee
    df_fee = df_fee.rename(columns={
        'orderNumber': 'order_number',
        'penaltyPay': 'late_fee',
        'numPay': 'installment_number'
    })
    df_fee = df_fee.drop_duplicates()
    df['installment_number'] = pd.to_numeric(df['installment_number'], errors='coerce').astype('Int64')
    df_fee['installment_number'] = pd.to_numeric(df_fee['installment_number'], errors='coerce').astype('Int64')
    df = pd.merge(df, df_fee, on=['order_number', 'installment_number'], how='left')
    df['late_fee'] = df['late_fee'].fillna(0).astype(int)

    # 7. คำนวณ total_paid
    def calc_paid(row):
        # ใช้ payment_amount ถ้ามี ถ้าไม่มีให้ใช้ installment_amount
        payment = pd.to_numeric(row['payment_amount'], errors='coerce')
        installment = pd.to_numeric(row['installment_amount'], errors='coerce')
        fee = row['late_fee']
        
        # ถ้ามี payment_amount ให้ใช้ payment_amount + late_fee
        # ถ้าไม่มี payment_amount ให้ใช้ installment_amount + late_fee
        base_amount = payment if pd.notna(payment) else (installment if pd.notna(installment) else 0)
        fee_amount = fee if pd.notna(fee) else 0
        
        # ถ้า late_fee = 0 และไม่มี payment_amount ให้ใช้ installment_amount
        if fee_amount == 0 and pd.isna(payment):
            return installment if pd.notna(installment) else 0
        
        return base_amount + fee_amount

    df['total_paid'] = df.apply(calc_paid, axis=1)

    # 8. payment_status
    df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')

    def status(row):
        if pd.isna(row['payment_amount']) and pd.isna(row['payment_date']) and pd.isna(row['payment_proof']):
            if pd.notna(row['due_date']) and datetime.now().date() > row['due_date'].date():
                return 'เกินกำหนดชำระ'
            return 'ยังไม่ชำระ'
        elif pd.notna(row['payment_amount']) or pd.notna(row['payment_proof']):
            if pd.notna(row['payment_date']) and pd.notna(row['due_date']):
                return 'ชำระแล้ว' if row['payment_date'].date() <= row['due_date'].date() else 'ชำระล่าช้า'
            return 'ชำระแล้ว'
        return 'ยังไม่ชำระ'

    df['payment_status'] = df.apply(status, axis=1)

    # 9. ล้าง test
    df = df[~df['quo_num'].isin(df_test['quo_num'])]
    df = df.rename(columns={'quo_num': 'quotation_num'})
    df['installment_number'] = df['installment_number'].replace({'0': '1'})
    df['installment_number'] = pd.to_numeric(df['installment_number'], errors='coerce').astype('Int64')
    df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')
    df['due_date'] = df['due_date'].dt.strftime('%Y%m%d').astype('Int64')
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
    df['payment_date'] = df['payment_date'].dt.strftime('%Y%m%d').astype('Int64')
    # แปลง NULL, NaN ที่เป็น string ให้เป็น None
    def clean_null_values(x):
        if isinstance(x, str):
            x_clean = x.strip().lower()
            if x_clean in ['null', 'nan', 'none', '']:
                return None
        return x
    
    df = df.applymap(clean_null_values)
    df = df.where(pd.notnull(df), None)

    return df

@op
def load_installment_data(df: pd.DataFrame):
    table_name = 'fact_installment_payments'
    pk_column = 'quotation_num'

    # ✅ กรอง fact_installment_payments ซ้ำจาก DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()

    # ✅ Load ข้อมูลเดิมจาก PostgreSQL
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    # ✅ กรอง fact_installment_payments ซ้ำจากข้อมูลเก่า
    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    # ✅ Identify fact_installment_payments ใหม่ (ไม่มีใน DB)
    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    # ✅ Identify fact_installment_payments ที่มีอยู่แล้ว
    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    # ✅ Merge ด้วย suffix (_new, _old)
    merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

    # ✅ ระบุคอลัมน์ที่ใช้เปรียบเทียบ (ยกเว้น key และ audit fields)
    exclude_columns = [pk_column, 'installment_id', 'create_at', 'update_at']
    compare_cols = [
        col for col in df.columns
        if col not in exclude_columns
        and f"{col}_new" in merged.columns
        and f"{col}_old" in merged.columns
    ]

    # ✅ ฟังก์ชันเปรียบเทียบอย่างปลอดภัยจาก pd.NA
    def is_different(row):
        for col in compare_cols:
            val_new = row.get(f"{col}_new")
            val_old = row.get(f"{col}_old")
            if pd.isna(val_new) and pd.isna(val_old):
                continue
            if val_new != val_old:
                return True
        return False

    # ✅ ตรวจหาความแตกต่างจริง
    df_diff = merged[merged.apply(is_different, axis=1)].copy()

    # ✅ เตรียม DataFrame สำหรับ update โดยใช้ fact_installment_payments ปกติ (ไม่เติม _new)
    update_cols = [f"{col}_new" for col in compare_cols]
    all_cols = [pk_column] + update_cols

    df_diff_renamed = df_diff[all_cols].copy()
    df_diff_renamed.columns = [pk_column] + compare_cols  # เปลี่ยนชื่อ column ให้ตรงกับตารางจริง

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

    # ✅ Load table metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert (กรอง fact_installment_payments ที่เป็น NaN)
    if not df_to_insert.empty:
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} insert rows with null fact_installment_payments")
        if not df_to_insert_valid.empty:
            with target_engine.begin() as conn:
                conn.execute(metadata.insert(), df_to_insert_valid.to_dict(orient='records'))

    # ✅ Update
    if not df_diff_renamed.empty:
        with target_engine.begin() as conn:
            for record in df_diff_renamed.to_dict(orient='records'):
                stmt = pg_insert(metadata).values(**record)
                update_columns = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns
                    if c.name != pk_column
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")

@job
def fact_installment_payments_etl():
    load_installment_data(clean_installment_data(extract_installment_data()))

if __name__ == "__main__":
    # ✅ Unpack tuple
    df_plan, df_installment, df_order, df_finance, df_bill, df_late_fee, df_test = extract_installment_data()
    
    print("✅ Extracted logs:")
    print(f"- df_plan: {df_plan.shape}")
    print(f"- df_installment: {df_installment.shape}")
    print(f"- df_order: {df_order.shape}")
    print(f"- df_finance: {df_finance.shape}")
    print(f"- df_bill: {df_bill.shape}")
    print(f"- df_late_fee: {df_late_fee.shape}")
    print(f"- df_test: {df_test.shape}")
    
    # ✅ Pass as tuple to cleaning function
    df_clean = clean_installment_data((df_plan, df_installment, df_order, df_finance, df_bill, df_late_fee, df_test))
    print("✅ Cleaned columns:", df_clean.columns)

    output_path = "fact_installment_payment.csv"
    df_clean.to_csv(output_path, index=False)
    print(f"💾 Saved to {output_path}")

    # load_installment_data(df_clean)
    # print("🎉 completed! Data upserted to fact_installment_payment.")