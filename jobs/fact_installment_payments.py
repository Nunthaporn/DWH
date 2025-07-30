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
    # ✅ ใช้ context manager เพื่อจัดการ connection อย่างปลอดภัย
    try:
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
        
    except Exception as e:
        print(f"❌ Error during data extraction: {e}")
        # ✅ Rollback connections เพื่อแก้ปัญหา PendingRollbackError
        try:
            with source_engine.connect() as conn:
                conn.rollback()
        except:
            pass
        try:
            with task_engine.connect() as conn:
                conn.rollback()
        except:
            pass
        try:
            with target_engine.connect() as conn:
                conn.rollback()
        except:
            pass
        raise e

    # ✅ Debug print
    print("📦 df_plan:", df_plan.shape)
    print("📦 df_installment:", df_installment.shape)
    print("📦 df_order:", df_order.shape)
    print("📦 df_finance:", df_finance.shape)
    print("📦 df_bill:", df_bill.shape)
    print("📦 df_late_fee:", df_late_fee.shape)
    print("📦 df_test:", df_test.shape)

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
    
    df_fin['numpay'] = pd.to_numeric(df_fin['numpay'], errors='coerce')
    df_fin = df_fin[df_fin['numpay'].notna() & (df_fin['numpay'] > 0)]

    # ใช้ vectorized operations แทน iterrows()
    rows_list = []
    for i, sfx in enumerate(num_to_name, start=1):
        money_col = f'moneypay_{sfx}'
        date_col = f'datepay_{sfx}'
        
        # สร้าง DataFrame สำหรับแต่ละ installment
        temp_df = df_fin[['order_number', money_col, date_col]].copy()
        temp_df['installment_number'] = i
        temp_df['payment_amount'] = temp_df[money_col]
        
        # ทำความสะอาดวันที่
        temp_df['raw_date'] = temp_df[date_col].astype(str)
        temp_df['payment_date'] = pd.to_datetime(
            temp_df['raw_date'].str.extract(r'(\d{4}-\d{1,2}-\d{1,2})')[0], 
            errors='coerce'
        )
        
        # แก้ไขปี 2026 เป็น 2025 - ใช้ vectorized operations
        mask_2026 = temp_df['payment_date'].dt.year == 2026
        if mask_2026.any():
            # ใช้ pd.to_datetime() แทน apply
            temp_df.loc[mask_2026, 'payment_date'] = pd.to_datetime(
                temp_df.loc[mask_2026, 'payment_date'].dt.strftime('2025-%m-%d'),
                errors='coerce'
            )
        
        rows_list.append(temp_df[['order_number', 'payment_amount', 'payment_date', 'installment_number']])

    df_payment = pd.concat(rows_list, ignore_index=True)

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
    df['installment_number'] = pd.to_numeric(df['installment_number'], errors='coerce')
    df_fee['installment_number'] = pd.to_numeric(df_fee['installment_number'], errors='coerce')
    df = pd.merge(df, df_fee, on=['order_number', 'installment_number'], how='left')
    df['late_fee'] = df['late_fee'].fillna(0).astype(int)

    # 7. คำนวณ total_paid - ใช้ vectorized operations แทน apply
    df['payment_amount'] = pd.to_numeric(df['payment_amount'], errors='coerce')
    df['installment_amount'] = pd.to_numeric(df['installment_amount'], errors='coerce')
    df['late_fee'] = pd.to_numeric(df['late_fee'], errors='coerce').fillna(0)
    
    # ใช้ numpy.where สำหรับ vectorized conditional logic
    import numpy as np
    
    # ถ้า late_fee = 0 ให้ใช้ installment_amount
    df['total_paid'] = np.where(
        df['late_fee'] == 0,
        df['installment_amount'].fillna(0),
        # ถ้า late_fee != 0 ให้ใช้ payment_amount + late_fee หรือ installment_amount + late_fee
        np.where(
            df['payment_amount'].notna(),
            df['payment_amount'] + df['late_fee'],
            df['installment_amount'].fillna(0) + df['late_fee']
        )
    )

    # 8. payment_status
    df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
    
    # สร้างเงื่อนไขต่างๆ
    no_payment = df['payment_amount'].isna() & df['payment_date'].isna() & df['payment_proof'].isna()
    has_payment = df['payment_amount'].notna() | df['payment_proof'].notna()
    has_due_date = df['due_date'].notna()
    has_payment_date = df['payment_date'].notna()
    is_overdue = has_due_date & (datetime.now().date() > df['due_date'].dt.date)
    is_late_payment = has_payment_date & has_due_date & (df['payment_date'].dt.date > df['due_date'].dt.date)
    
    # กำหนด payment_status
    df['payment_status'] = 'ยังไม่ชำระ'  # ค่าเริ่มต้น
    df.loc[no_payment & is_overdue, 'payment_status'] = 'เกินกำหนดชำระ'
    df.loc[has_payment & ~is_late_payment, 'payment_status'] = 'ชำระแล้ว'
    df.loc[has_payment & is_late_payment, 'payment_status'] = 'ชำระล่าช้า'

    # 9. ล้าง test
    df = df[~df['quo_num'].isin(df_test['quo_num'])]
    df = df.rename(columns={'quo_num': 'quotation_num'})
    df['installment_number'] = df['installment_number'].replace({'0': '1'})
    df['installment_number'] = pd.to_numeric(df['installment_number'], errors='coerce')
    df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')
    df['due_date'] = df['due_date'].dt.strftime('%Y%m%d')
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
    df['payment_date'] = df['payment_date'].dt.strftime('%Y%m%d')
    
    # ✅ ตรวจสอบข้อมูลก่อนการทำความสะอาด - ลดการตรวจสอบ
    print("🔍 Before cleaning:")
    print(f"📊 Shape: {df.shape}")
    nan_counts_before = df.isna().sum()
    print("📊 NaN counts before cleaning:")
    for col, count in nan_counts_before.items():
        if count > 0:
            print(f"  - {col}: {count}")
    
    # ✅ ใช้ sanitize_dataframe function เพื่อทำความสะอาดข้อมูลอย่างครอบคลุม - เรียกครั้งเดียว
    print("🧹 Applying comprehensive data sanitization...")
    df = sanitize_dataframe(df.copy())
    
    # ✅ ตรวจสอบผลลัพธ์การทำความสะอาด - ลดการตรวจสอบ
    print("✅ After cleaning:")
    print(f"📊 Shape: {df.shape}")
    
    # ตรวจสอบ NaN values ที่เหลือ
    nan_counts_after = df.isna().sum()
    print("📊 NaN counts after cleaning:")
    for col, count in nan_counts_after.items():
        if count > 0:
            print(f"  - {col}: {count}")
    
    # แสดงสรุปการเปลี่ยนแปลง
    print("\n📊 Cleaning completed")
    for col in df.columns:
        if col in nan_counts_before.index and col in nan_counts_after.index:
            before = nan_counts_before[col]
            after = nan_counts_after[col]
            if before != after:
                print(f"  - {col}: {before} → {after} NaN values")

    return df

def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """ล้างค่า 'NaN', 'null', 'none' string ให้เป็น None และแปลง float NaN เป็น None - เวอร์ชันที่ปรับปรุงประสิทธิภาพ"""
    
    # 🔁 สร้าง copy เพื่อไม่ให้กระทบข้อมูลต้นฉบับ
    df_clean = df.copy()
    
    # 🔁 แปลง float NaN เป็น None ก่อน
    df_clean = df_clean.where(pd.notna(df_clean), None)
    
    # 🔁 ล้าง string columns อย่างมีประสิทธิภาพ
    string_columns = df_clean.select_dtypes(include=['object']).columns
    
    for col in string_columns:
        if col in df_clean.columns:
            # แปลงเป็น string และทำความสะอาดในครั้งเดียว
            df_clean[col] = df_clean[col].astype(str)
            
            # ใช้วิธีที่เร็วขึ้น - ตรวจสอบเฉพาะค่าที่จำเป็น
            nan_mask = (
                df_clean[col].str.lower().isin(['nan', 'null', 'none', 'nonetype', 'nulltype']) |
                df_clean[col].str.strip().isin(['', '[null]']) |
                df_clean[col].str.contains('^\\[null\\]$', case=False, na=False)
            )
            
            # แทนที่ค่า NaN string ด้วย None
            df_clean.loc[nan_mask, col] = None
            
            # ลบ whitespace เฉพาะค่าที่ไม่ใช่ None
            mask_not_none = df_clean[col].notna()
            if mask_not_none.any():
                df_clean.loc[mask_not_none, col] = df_clean.loc[mask_not_none, col].str.strip()
                
                # ตรวจสอบอีกครั้งหลังจาก strip
                nan_mask_after = (
                    df_clean[col].str.lower().isin(['nan', 'null', 'none', '']) |
                    df_clean[col].str.contains('^\\[null\\]$', case=False, na=False)
                )
                df_clean.loc[nan_mask_after, col] = None

    # 🔁 ล้างคอลัมน์ตัวเลขที่อาจมี comma - ใช้วิธีที่เร็วขึ้น
    numeric_cols = ['installment_amount', 'payment_amount', 'total_paid', 'late_fee']
    for col in numeric_cols:
        if col in df_clean.columns:
            # ลบ comma เฉพาะ cell ที่เป็น string และไม่ใช่ None
            mask_string = df_clean[col].astype(str).str.contains(',', na=False)
            if mask_string.any():
                df_clean.loc[mask_string, col] = df_clean.loc[mask_string, col].astype(str).str.replace(',', '')
            
            # แปลงเป็น numeric
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            # แปลง NaN เป็น None
            df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)

    return df_clean

@op
def load_installment_data(df: pd.DataFrame):
    table_name = 'fact_installment_payments'
    pk_column = 'quotation_num'

    # ✅ ตรวจสอบข้อมูลก่อนการบันทึก - ลดการตรวจสอบที่ซ้ำซ้อน
    print("🔍 Before database operations:")
    print(f"📊 DataFrame shape: {df.shape}")
    
    # ตรวจสอบ NaN values เฉพาะครั้งเดียว
    nan_counts = df.isna().sum()
    print("📊 NaN counts before DB operations:")
    for col, count in nan_counts.items():
        if count > 0:
            print(f"  - {col}: {count}")
    
    # ✅ ใช้ sanitize_dataframe function เพื่อทำความสะอาดข้อมูลอย่างครอบคลุม - เรียกครั้งเดียว
    print("🧹 Applying comprehensive data sanitization...")
    df = sanitize_dataframe(df.copy())
    
    # ✅ ตรวจสอบข้อมูลหลังการทำความสะอาด - ลดการตรวจสอบ
    print("✅ After sanitization:")
    nan_counts_after = df.isna().sum()
    print("📊 NaN counts after sanitization:")
    for col, count in nan_counts_after.items():
        if count > 0:
            print(f"  - {col}: {count}")

    # ✅ กรอง fact_installment_payments ซ้ำจาก DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()

    # ✅ Load ข้อมูลเดิมจาก PostgreSQL
    try:
        with target_engine.connect() as conn:
            df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    except Exception as e:
        print(f"❌ Error loading existing data: {e}")
        # ✅ Rollback connection
        try:
            with target_engine.connect() as conn:
                conn.rollback()
        except:
            pass
        raise e

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

    # ✅ ตรวจหาความแตกต่างจริง - ใช้ vectorized operations แทน apply
    # สร้าง mask สำหรับแต่ละคอลัมน์
    diff_mask = pd.Series([False] * len(merged), index=merged.index)
    
    for col in compare_cols:
        col_new = f"{col}_new"
        col_old = f"{col}_old"
        
        # เปรียบเทียบคอลัมน์
        col_diff = (merged[col_new] != merged[col_old]) & (
            merged[col_new].notna() | merged[col_old].notna()
        )
        diff_mask = diff_mask | col_diff
    
    df_diff = merged[diff_mask].copy()

    # ✅ เตรียม DataFrame สำหรับ update โดยใช้ fact_installment_payments ปกติ (ไม่เติม _new)
    update_cols = [f"{col}_new" for col in compare_cols]
    all_cols = [pk_column] + update_cols

    df_diff_renamed = df_diff[all_cols].copy()
    df_diff_renamed.columns = [pk_column] + compare_cols  # เปลี่ยนชื่อ column ให้ตรงกับตารางจริง

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

    # ✅ ตรวจสอบและทำความสะอาดข้อมูลก่อนการ insert/update - ลดการตรวจสอบ
    if not df_to_insert.empty:
        print("🔍 Checking insert data:")
        insert_nan_counts = df_to_insert.isna().sum()
        for col, count in insert_nan_counts.items():
            if count > 0:
                print(f"  - {col}: {count} NaN values")
        
        # ✅ ทำความสะอาดข้อมูล insert อีกครั้ง
        print("🧹 Sanitizing insert data...")
        df_to_insert = sanitize_dataframe(df_to_insert.copy())
    
    if not df_diff_renamed.empty:
        print("🔍 Checking update data:")
        update_nan_counts = df_diff_renamed.isna().sum()
        for col, count in update_nan_counts.items():
            if count > 0:
                print(f"  - {col}: {count} NaN values")
        
        # ✅ ทำความสะอาดข้อมูล update อีกครั้ง
        print("🧹 Sanitizing update data...")
        df_diff_renamed = sanitize_dataframe(df_diff_renamed.copy())

    # ✅ Load table metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert (กรอง fact_installment_payments ที่เป็น NaN)
    if not df_to_insert.empty:
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped}")
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

    # ✅ ตรวจสอบข้อมูลในฐานข้อมูลหลังการ insert/update - ลดการตรวจสอบ
    print("🔍 Checking data in database after operations:")
    try:
        with target_engine.connect() as conn:
            # ตรวจสอบจำนวนแถวทั้งหมด
            total_rows = pd.read_sql(f"SELECT COUNT(*) as total FROM {table_name}", conn).iloc[0]['total']
            print(f"📊 Total rows in {table_name}: {total_rows}")
            
            # ตรวจสอบ NaN values ในฐานข้อมูล - เฉพาะคอลัมน์หลัก
            key_columns = ['quotation_num', 'installment_number', 'order_number', 'payment_status']
            for col in key_columns:
                if col in df.columns:
                    null_count = pd.read_sql(f"SELECT COUNT(*) as null_count FROM {table_name} WHERE {col} IS NULL", conn).iloc[0]['null_count']
                    if null_count > 0:
                        print(f"  - {col}: {null_count} NULL values in database")
    except Exception as e:
        print(f"❌ Error in database check: {e}")
        # ✅ Rollback connection
        try:
            with target_engine.connect() as conn:
                conn.rollback()
        except:
            pass
        raise e

    print("✅ Insert/update completed.")
    
    # ✅ ทำความสะอาดข้อมูลในฐานข้อมูลที่มีอยู่แล้ว (ถ้ามี NaN strings) - ลดการตรวจสอบ
    print("🧹 Cleaning existing data in database...")
    with target_engine.begin() as conn:
        # ตรวจสอบประเภทข้อมูลของคอลัมน์ในฐานข้อมูล
        column_info_query = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        column_info = pd.read_sql(column_info_query, conn)
        
        # อัปเดตค่า NaN strings ให้เป็น NULL (เฉพาะคอลัมน์ string)
        update_queries = []
        for col in df.columns:
            col_info = column_info[column_info['column_name'] == col]
            if not col_info.empty:
                data_type = col_info.iloc[0]['data_type']
                
                if data_type in ['character varying', 'text', 'character']:
                    update_query = f"""
                    UPDATE {table_name} 
                    SET {col} = NULL 
                    WHERE {col} IN ('nan', 'NaN', 'NAN', 'null', 'NULL', 'Null', 'none', 'None', 'NONE', '[null]', '[NULL]', '[Null]')
                    OR {col} = ''
                    """
                    update_queries.append(update_query)
        
        # ทำการอัปเดต
        for query in update_queries:
            try:
                result = conn.execute(query)
                if result.rowcount > 0:
                    print(f"  ✅ Updated {result.rowcount} rows with NaN strings")
            except Exception as e:
                print(f"  ⚠️ Warning: Could not update some rows: {e}")
    
    # ✅ ตรวจสอบข้อมูลในฐานข้อมูลหลังการทำความสะอาด - ลดการตรวจสอบ
    print("🔍 Final check of data in database after cleaning:")
    try:
        with target_engine.connect() as conn:
            # ตรวจสอบจำนวนแถวทั้งหมด
            total_rows = pd.read_sql(f"SELECT COUNT(*) as total FROM {table_name}", conn).iloc[0]['total']
            print(f"📊 Total rows in {table_name}: {total_rows}")
            
            # ตรวจสอบ NaN values ในฐานข้อมูล - เฉพาะคอลัมน์หลัก
            key_columns = ['quotation_num', 'installment_number', 'order_number', 'payment_status']
            for col in key_columns:
                if col in df.columns:
                    null_count = pd.read_sql(f"SELECT COUNT(*) as null_count FROM {table_name} WHERE {col} IS NULL", conn).iloc[0]['null_count']
                    if null_count > 0:
                        print(f"  - {col}: {null_count} NULL values in database")
    
    except Exception as e:
        print(f"❌ Error in final database check: {e}")
        # ✅ Rollback connection
        try:
            with target_engine.connect() as conn:
                conn.rollback()
        except:
            pass
        raise e

@job
def fact_installment_payments_etl():
    load_installment_data(clean_installment_data(extract_installment_data()))

if __name__ == "__main__":
    try:
        # ✅ Unpack tuple
        df_plan, df_installment, df_order, df_finance, df_bill, df_late_fee, df_test = extract_installment_data()
        
        # ✅ Pass as tuple to cleaning function
        df_clean = clean_installment_data((df_plan, df_installment, df_order, df_finance, df_bill, df_late_fee, df_test))
        
        # ✅ ตรวจสอบข้อมูลก่อนการบันทึก - ลดการตรวจสอบที่ซ้ำซ้อน
        print("🔍 Before database operations:")
        print(f"📊 DataFrame shape: {df_clean.shape}")
        
        # ตรวจสอบ NaN values เฉพาะครั้งเดียว
        nan_counts_csv = df_clean.isna().sum()
        print("📊 NaN counts before DB operations:")
        for col, count in nan_counts_csv.items():
            if count > 0:
                print(f"  - {col}: {count}")
        
        # ✅ ใช้ sanitize_dataframe function เพื่อทำความสะอาดข้อมูลอย่างครอบคลุม - เรียกครั้งเดียว
        print("🧹 Applying comprehensive data sanitization...")
        df_clean = sanitize_dataframe(df_clean.copy())
        
        # ✅ ตรวจสอบข้อมูลหลังการทำความสะอาด - ลดการตรวจสอบ
        print("✅ After sanitization:")
        nan_counts_after = df_clean.isna().sum()
        print("📊 NaN counts after sanitization:")
        for col, count in nan_counts_after.items():
            if count > 0:
                print(f"  - {col}: {count}")

        load_installment_data(df_clean)
        print("🎉 completed! Data upserted to fact_installment_payment.")
        
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        # ✅ Rollback connections เพื่อแก้ปัญหา PendingRollbackError
        try:
            with source_engine.connect() as conn:
                conn.rollback()
        except:
            pass
        try:
            with task_engine.connect() as conn:
                conn.rollback()
        except:
            pass
        try:
            with target_engine.connect() as conn:
                conn.rollback()
        except:
            pass
        raise e
    
