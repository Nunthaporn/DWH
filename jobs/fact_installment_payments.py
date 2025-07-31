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
            WHERE datestart >= '2025-01-01' AND datestart < '2025-07-31'
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
            # AND order_number REGEXP '[A-Z]+25[0-9]{2}-[0-9]+'
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
            # AND orderNumber REGEXP '[A-Z]+25[0-9]{2}-[0-9]+'
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

    # ✅ Debug: ตรวจสอบข้อมูลใน df_filtered
    print(f"🔍 df_filtered shape: {df_filtered.shape}")
    print(f"🔍 df_filtered columns: {list(df_filtered.columns)}")
    
    # ตรวจสอบข้อมูลในคอลัมน์ money
    for col in money_cols[:3]:  # ตรวจสอบแค่ 3 คอลัมน์แรก
        if col in df_filtered.columns:
            non_null_count = df_filtered[col].notna().sum()
            print(f"🔍 {col}: {non_null_count} non-null values")
            if non_null_count > 0:
                print(f"🔍 Sample {col} values: {df_filtered[col].dropna().head(3).tolist()}")

    df_money = df_filtered.melt(id_vars=['quo_num', 'numpay'], value_vars=money_cols,
                                 var_name='installment_period', value_name='installment_amount')
    df_date = df_filtered.melt(id_vars=['quo_num', 'numpay'], value_vars=date_cols,
                                var_name='due_date_period', value_name='due_date')

    # ✅ Debug: ตรวจสอบข้อมูลใน df_money
    print(f"🔍 df_money shape: {df_money.shape}")
    print(f"🔍 df_money installment_amount non-null: {df_money['installment_amount'].notna().sum()}")
    if df_money['installment_amount'].notna().sum() > 0:
        print(f"🔍 Sample installment_amount values: {df_money['installment_amount'].dropna().head(5).tolist()}")

    df_combined = pd.concat([df_money.reset_index(drop=True), df_date['due_date']], axis=1)
    df_combined['installment_number'] = df_combined.groupby('quo_num').cumcount() + 1
    df_combined = df_combined[df_combined['installment_number'] <= df_combined['numpay']]
    df_combined = df_combined.sort_values(by=['quo_num', 'due_date'])
    df_combined['installment_number'] = df_combined.groupby('quo_num').cumcount() + 1

    # ✅ Debug: ตรวจสอบข้อมูลใน df_combined
    print(f"🔍 df_combined shape: {df_combined.shape}")
    print(f"🔍 df_combined installment_amount non-null: {df_combined['installment_amount'].notna().sum()}")
    print(f"🔍 df_combined unique quo_num: {df_combined['quo_num'].nunique()}")

    # 2. ผูก order_number
    print(f"🔍 df_join before merge shape: {df_combined.shape}")
    print(f"🔍 df_order shape: {df_order.shape}")
    print(f"🔍 df_order sample:")
    print(df_order.head())
    
    # ✅ Debug: ตรวจสอบ intersection ของ quo_num
    combined_quos = set(df_combined['quo_num'].dropna())
    order_quos = set(df_order['quo_num'].dropna())
    common_quos = combined_quos & order_quos
    print(f"🔍 Common quo_num: {len(common_quos)}")
    print(f"🔍 df_combined only: {len(combined_quos - order_quos)}")
    print(f"🔍 df_order only: {len(order_quos - combined_quos)}")
    
    df_join = pd.merge(df_combined, df_order, on='quo_num', how='left')
    print(f"🔍 df_join after merge shape: {df_join.shape}")
    print(f"🔍 df_join order_number non-null: {df_join['order_number'].notna().sum()}")
    print(f"🔍 df_join installment_amount non-null: {df_join['installment_amount'].notna().sum()}")
    
    # ตรวจสอบข้อมูลที่ไม่มี order_number
    missing_order = df_join[df_join['order_number'].isna()]
    print(f"🔍 Missing order_number: {len(missing_order)} rows")
    if not missing_order.empty:
        print("🔍 Sample missing order_number:")
        print(missing_order[['quo_num', 'installment_number', 'installment_amount']].head())

    # 3. เตรียมข้อมูลการชำระ
    num_to_name = ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight',
                   'nine', 'ten', 'eleven', 'twelve']
    
    df_fin['numpay'] = pd.to_numeric(df_fin['numpay'], errors='coerce')
    df_fin = df_fin[df_fin['numpay'].notna() & (df_fin['numpay'] > 0)]

    # ✅ Debug: ตรวจสอบข้อมูลใน df_fin
    print(f"🔍 df_fin shape: {df_fin.shape}")
    print(f"🔍 df_fin columns: {list(df_fin.columns)}")
    
    # ตรวจสอบรูปแบบ order_number ใน df_fin
    print("🔍 df_fin order_number patterns:")
    df_fin_sample = df_fin['order_number'].dropna().head(10)
    for order in df_fin_sample:
        print(f"  - {order}")
    
    # ตรวจสอบรูปแบบ order_number ใน df_order
    print("🔍 df_order order_number patterns:")
    df_order_sample = df_order['order_number'].dropna().head(10)
    for order in df_order_sample:
        print(f"  - {order}")
    
    # ตรวจสอบข้อมูลในคอลัมน์ payment
    for sfx in num_to_name[:3]:  # ตรวจสอบแค่ 3 คอลัมน์แรก
        money_col = f'moneypay_{sfx}'
        date_col = f'datepay_{sfx}'
        if money_col in df_fin.columns:
            non_null_money = df_fin[money_col].notna().sum()
            print(f"🔍 {money_col}: {non_null_money} non-null values")
            if non_null_money > 0:
                print(f"🔍 Sample {money_col} values: {df_fin[money_col].dropna().head(3).tolist()}")
        if date_col in df_fin.columns:
            non_null_date = df_fin[date_col].notna().sum()
            print(f"🔍 {date_col}: {non_null_date} non-null values")

    # ใช้ vectorized operations แทน iterrows()
    rows_list = []
    for i, sfx in enumerate(num_to_name, start=1):
        money_col = f'moneypay_{sfx}'
        date_col = f'datepay_{sfx}'
        
        # สร้าง DataFrame สำหรับแต่ละ installment
        temp_df = df_fin[['order_number', money_col, date_col]].copy()
        temp_df['installment_number'] = i
        temp_df['payment_amount'] = temp_df[money_col]
        
        # ✅ Debug: ตรวจสอบข้อมูลใน temp_df
        if i <= 3:  # ตรวจสอบแค่ 3 แถวแรก
            print(f"🔍 Installment {i}:")
            print(f"  - temp_df shape: {temp_df.shape}")
            print(f"  - payment_amount non-null: {temp_df['payment_amount'].notna().sum()}")
            print(f"  - {date_col} non-null: {temp_df[date_col].notna().sum()}")
            if temp_df['payment_amount'].notna().sum() > 0:
                print(f"  - Sample payment_amount values: {temp_df['payment_amount'].dropna().head(3).tolist()}")
        
        # ทำความสะอาดวันที่
        temp_df['raw_date'] = temp_df[date_col].astype(str)
        temp_df['payment_date'] = pd.to_datetime(
            temp_df['raw_date'].str.extract(r'(\d{4}-\d{1,2}-\d{1,2})')[0], 
            errors='coerce'
        )
        
        # ✅ Debug: ตรวจสอบการแปลงวันที่
        if i <= 3:
            print(f"  - payment_date non-null after conversion: {temp_df['payment_date'].notna().sum()}")
        
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
    
    # ✅ Debug: ตรวจสอบข้อมูลใน df_payment
    print(f"🔍 df_payment shape: {df_payment.shape}")
    print(f"🔍 df_payment payment_amount non-null: {df_payment['payment_amount'].notna().sum()}")
    print(f"🔍 df_payment payment_date non-null: {df_payment['payment_date'].notna().sum()}")
    
    # ตรวจสอบ order_number ใน df_payment
    print(f"🔍 df_payment order_number non-null: {df_payment['order_number'].notna().sum()}")
    print(f"🔍 df_payment order_number unique: {df_payment['order_number'].nunique()}")
    
    # ตรวจสอบ order_number ใน df_join
    print(f"🔍 df_join order_number non-null: {df_join['order_number'].notna().sum()}")
    print(f"🔍 df_join order_number unique: {df_join['order_number'].nunique()}")
    
    # ตรวจสอบ intersection ของ order_number
    join_orders = set(df_join['order_number'].dropna())
    payment_orders = set(df_payment['order_number'].dropna())
    common_orders = join_orders & payment_orders
    print(f"🔍 Common order_number: {len(common_orders)}")
    print(f"🔍 df_join only: {len(join_orders - payment_orders)}")
    print(f"🔍 df_payment only: {len(payment_orders - join_orders)}")
    
    # แสดงตัวอย่างข้อมูล
    if not df_payment.empty:
        print("🔍 Sample df_payment data:")
        sample_data = df_payment.head(5)
        for idx, row in sample_data.iterrows():
            print(f"  Row {idx}: order_number={row['order_number']}, "
                  f"payment_amount={row['payment_amount']}, "
                  f"payment_date={row['payment_date']}")

    # 4. เตรียม payment proof
    df_proof = df_bill.melt(id_vars=['order_number'],
        value_vars=[f'bill_receipt{i if i > 1 else ""}' for i in range(1, 13)],
        value_name='payment_proof')
    df_proof = df_proof[df_proof['payment_proof'].notna()].reset_index()
    df_proof['installment_number'] = df_proof.groupby('order_number').cumcount() + 1
    df_proof = df_proof[['order_number', 'installment_number', 'payment_proof']]

    # 5. รวมทั้งหมด
    print(f"🔍 Before merge - df_join shape: {df_join.shape}")
    print(f"🔍 Before merge - df_payment shape: {df_payment.shape}")
    print(f"🔍 Before merge - df_proof shape: {df_proof.shape}")
    print(f"🔍 Before merge - df_join installment_amount non-null: {df_join['installment_amount'].notna().sum()}")
    
    # ✅ แปลง installment_number เป็น numeric ก่อน merge
    df_join['installment_number'] = pd.to_numeric(df_join['installment_number'], errors='coerce')
    df_payment['installment_number'] = pd.to_numeric(df_payment['installment_number'], errors='coerce')
    df_proof['installment_number'] = pd.to_numeric(df_proof['installment_number'], errors='coerce')
    
    df = pd.merge(df_join, df_payment, on=['order_number', 'installment_number'], how='left')
    print(f"🔍 After first merge - df shape: {df.shape}")
    print(f"🔍 After first merge - payment_amount non-null: {df['payment_amount'].notna().sum()}")
    print(f"🔍 After first merge - payment_date non-null: {df['payment_date'].notna().sum()}")
    print(f"🔍 After first merge - installment_amount non-null: {df['installment_amount'].notna().sum()}")
    
    df = pd.merge(df, df_proof, on=['order_number', 'installment_number'], how='left')
    print(f"🔍 After second merge - df shape: {df.shape}")
    print(f"🔍 After second merge - payment_amount non-null: {df['payment_amount'].notna().sum()}")
    print(f"🔍 After second merge - payment_date non-null: {df['payment_date'].notna().sum()}")
    print(f"🔍 After second merge - installment_amount non-null: {df['installment_amount'].notna().sum()}")

    # ✅ ตรวจสอบข้อมูลก่อนการตั้งค่า installment 1
    print(f"🔍 Before setting installment 1 - installment_amount non-null: {df['installment_amount'].notna().sum()}")
    print(f"🔍 Before setting installment 1 - payment_amount non-null: {df['payment_amount'].notna().sum()}")
    
    # ✅ แปลง installment_number เป็น numeric ก่อนเปรียบเทียบ
    df['installment_number'] = pd.to_numeric(df['installment_number'], errors='coerce')
    
    # ✅ ตรวจสอบข้อมูลก่อนการตั้งค่า
    installment_1_mask = (df['installment_number'] == 1) & (df['payment_amount'].isna())
    print(f"🔍 Rows where installment_number=1 and payment_amount is null: {installment_1_mask.sum()}")
    
    # ✅ ตั้งค่า payment_amount สำหรับ installment 1 ที่ยังไม่มีข้อมูล
    df.loc[installment_1_mask, 'payment_amount'] = df.loc[installment_1_mask, 'installment_amount']
    
    print(f"🔍 After setting installment 1 - payment_amount non-null: {df['payment_amount'].notna().sum()}")

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
    # ✅ ตรวจสอบข้อมูลก่อนการแปลง
    print(f"🔍 Before numeric conversion - installment_amount non-null: {df['installment_amount'].notna().sum()}")
    print(f"🔍 Before numeric conversion - payment_amount non-null: {df['payment_amount'].notna().sum()}")
    
    # ✅ ลบ comma ออกจากข้อมูลก่อนแปลงเป็น numeric
    if df['payment_amount'].dtype == 'object':
        print(f"🔍 payment_amount dtype: {df['payment_amount'].dtype}")
        print(f"🔍 payment_amount sample values before cleaning: {df['payment_amount'].dropna().head(5).tolist()}")
        
        # ทำความสะอาดข้อมูลก่อนลบ comma
        df['payment_amount'] = df['payment_amount'].astype(str)
        
        # ลบ comma เฉพาะค่าที่ไม่ใช่ NaN string
        mask_not_nan = ~df['payment_amount'].str.lower().isin(['nan', 'null', 'none', 'undefined'])
        df.loc[mask_not_nan, 'payment_amount'] = df.loc[mask_not_nan, 'payment_amount'].str.replace(',', '')
        
        print(f"🔍 payment_amount sample values after cleaning: {df['payment_amount'].dropna().head(5).tolist()}")
    
    if df['installment_amount'].dtype == 'object':
        print(f"🔍 installment_amount dtype: {df['installment_amount'].dtype}")
        
        # ทำความสะอาดข้อมูลก่อนลบ comma
        df['installment_amount'] = df['installment_amount'].astype(str)
        
        # ลบ comma เฉพาะค่าที่ไม่ใช่ NaN string
        mask_not_nan = ~df['installment_amount'].str.lower().isin(['nan', 'null', 'none', 'undefined'])
        df.loc[mask_not_nan, 'installment_amount'] = df.loc[mask_not_nan, 'installment_amount'].str.replace(',', '')
    
    # ✅ แปลงเป็น numeric และจัดการ NaN
    print(f"🔍 Before numeric conversion - payment_amount non-null: {df['payment_amount'].notna().sum()}")
    df['payment_amount'] = pd.to_numeric(df['payment_amount'], errors='coerce')
    print(f"🔍 After numeric conversion - payment_amount non-null: {df['payment_amount'].notna().sum()}")
    
    df['installment_amount'] = pd.to_numeric(df['installment_amount'], errors='coerce')
    df['late_fee'] = pd.to_numeric(df['late_fee'], errors='coerce').fillna(0)
    
    # ✅ ตรวจสอบข้อมูลหลังการแปลง
    print(f"🔍 After numeric conversion - installment_amount non-null: {df['installment_amount'].notna().sum()}")
    print(f"🔍 After numeric conversion - payment_amount non-null: {df['payment_amount'].notna().sum()}")
    
    # ✅ ตรวจสอบค่าที่เป็น Infinity หรือ -Infinity
    df['payment_amount'] = df['payment_amount'].replace([np.inf, -np.inf], np.nan)
    df['installment_amount'] = df['installment_amount'].replace([np.inf, -np.inf], np.nan)
    df['late_fee'] = df['late_fee'].replace([np.inf, -np.inf], 0)
    
    # ✅ คำนวณ total_paid อย่างปลอดภัย
    df['total_paid'] = np.where(
        df['late_fee'] == 0,
        df['installment_amount'].fillna(0),
        # ถ้า late_fee != 0 ให้ใช้ payment_amount + late_fee หรือ installment_amount + late_fee
        np.where(
            df['payment_amount'].notna(),
            df['payment_amount'].fillna(0) + df['late_fee'],
            df['installment_amount'].fillna(0) + df['late_fee']
        )
    )
    
    # ✅ ตรวจสอบ total_paid ที่คำนวณได้
    print(f"🔍 total_paid non-null: {df['total_paid'].notna().sum()}")
    print(f"🔍 total_paid range: {df['total_paid'].min()} to {df['total_paid'].max()}")

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

    # 9. ล้าง test และ undefined
    df = df[~df['quo_num'].isin(df_test['quo_num'])]
    
    # ลบแถวที่มี quo_num เป็น undefined
    df = df[df['quo_num'] != 'undefined']
    df = df[df['quo_num'].notna()]  # ลบแถวที่มี quo_num เป็น null/NaN ด้วย
    
    df = df.rename(columns={'quo_num': 'quotation_num'})
    df['installment_number'] = df['installment_number'].replace({'0': '1'})
    df['installment_number'] = pd.to_numeric(df['installment_number'], errors='coerce')
    
    # ✅ แปลงวันที่อย่างปลอดภัย
    print(f"🔍 Before due_date conversion - non-null: {df['due_date'].notna().sum()}")
    print(f"🔍 due_date sample values: {df['due_date'].dropna().head(3).tolist()}")
    df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')
    print(f"🔍 After due_date conversion - non-null: {df['due_date'].notna().sum()}")
    # ✅ ตรวจสอบวันที่ที่แปลงไม่ได้
    invalid_due_dates = df[df['due_date'].isna()]['due_date'].shape[0]
    if invalid_due_dates > 0:
        print(f"⚠️ Invalid due_date values: {invalid_due_dates}")
    
    df['due_date'] = df['due_date'].dt.strftime('%Y%m%d')
    print(f"🔍 After due_date strftime - non-null: {df['due_date'].notna().sum()}")
    
    print(f"🔍 Before payment_date conversion - non-null: {df['payment_date'].notna().sum()}")
    print(f"🔍 payment_date sample values: {df['payment_date'].dropna().head(3).tolist()}")
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
    print(f"🔍 After payment_date conversion - non-null: {df['payment_date'].notna().sum()}")
    # ✅ ตรวจสอบวันที่ที่แปลงไม่ได้
    invalid_payment_dates = df[df['payment_date'].isna()]['payment_date'].shape[0]
    if invalid_payment_dates > 0:
        print(f"⚠️ Invalid payment_date values: {invalid_payment_dates}")
    
    df['payment_date'] = df['payment_date'].dt.strftime('%Y%m%d')
    print(f"🔍 After payment_date strftime - non-null: {df['payment_date'].notna().sum()}")
    
    # ✅ ตรวจสอบข้อมูลก่อนการทำความสะอาด - ลดการตรวจสอบ
    print("🔍 Before cleaning:")
    print(f"📊 Shape: {df.shape}")
    print(f"📊 installment_amount non-null: {df['installment_amount'].notna().sum()}")
    print(f"📊 payment_amount non-null: {df['payment_amount'].notna().sum()}")
    print(f"📊 payment_amount sample values: {df['payment_amount'].dropna().head(5).tolist()}")
    print(f"📊 payment_amount dtype: {df['payment_amount'].dtype}")
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
    print(f"📊 installment_amount non-null: {df['installment_amount'].notna().sum()}")
    print(f"📊 payment_amount non-null: {df['payment_amount'].notna().sum()}")
    print(f"📊 payment_amount sample values: {df['payment_amount'].dropna().head(5).tolist()}")
    print(f"📊 payment_amount dtype: {df['payment_amount'].dtype}")
    
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

    # ✅ เลือกเฉพาะคอลัมน์ที่ต้องการสำหรับตารางปลายทาง
    final_columns = [
        'quotation_num', 'installment_number', 'due_date', 'installment_amount',
        'payment_date', 'payment_amount', 'late_fee', 'total_paid', 
        'payment_proof', 'payment_status', 'order_number'
    ]
    
    # ตรวจสอบว่าคอลัมน์ที่ต้องการมีอยู่หรือไม่
    available_columns = [col for col in final_columns if col in df.columns]
    missing_columns = [col for col in final_columns if col not in df.columns]
    
    if missing_columns:
        print(f"⚠️ Missing columns: {missing_columns}")
    
    df_final = df[available_columns].copy()
    print(f"📊 Final DataFrame shape: {df_final.shape}")
    print(f"📊 Final columns: {list(df_final.columns)}")
    print(f"📊 Final installment_amount non-null: {df_final['installment_amount'].notna().sum()}")
    print(f"📊 Final payment_amount non-null: {df_final['payment_amount'].notna().sum()}")
    print(f"📊 Final payment_amount sample values: {df_final['payment_amount'].dropna().head(5).tolist()}")
    print(f"📊 Final payment_amount dtype: {df_final['payment_amount'].dtype}")
    
    # ✅ ตรวจสอบข้อมูลสุดท้ายก่อนส่งกลับ
    print("\n📊 Final data quality check:")
    for col in df_final.columns:
        non_null_count = df_final[col].notna().sum()
        total_count = len(df_final)
        null_percentage = (total_count - non_null_count) / total_count * 100
        print(f"  - {col}: {non_null_count}/{total_count} ({null_percentage:.1f}% null)")
        
        # ตรวจสอบค่าที่เป็น string 'nan' หรือ 'null'
        if df_final[col].dtype == 'object':
            nan_strings = df_final[col].astype(str).str.lower().isin(['nan', 'null', 'none', 'undefined']).sum()
            if nan_strings > 0:
                print(f"    ⚠️ Contains {nan_strings} string NaN/null values")
        
        # ตรวจสอบข้อมูลในคอลัมน์ payment_amount อย่างละเอียด
        if col == 'payment_amount':
            print(f"    🔍 payment_amount dtype: {df_final[col].dtype}")
            print(f"    🔍 payment_amount sample values: {df_final[col].dropna().head(3).tolist()}")
            print(f"    🔍 payment_amount unique values: {df_final[col].dropna().nunique()}")
    
    # ✅ แปลง NaN string เป็น None สำหรับ PostgreSQL
    for col in df_final.columns:
        if df_final[col].dtype == 'object':
            df_final[col] = df_final[col].replace(['nan', 'null', 'none', 'undefined', 'NaN', 'NULL', 'NONE', 'UNDEFINED'], None)
    
    print(f"📊 After NaN string replacement - payment_amount non-null: {df_final['payment_amount'].notna().sum()}")
    print(f"📊 After NaN string replacement - payment_amount sample values: {df_final['payment_amount'].dropna().head(5).tolist()}")
    print(f"📊 After NaN string replacement - payment_amount dtype: {df_final['payment_amount'].dtype}")
    
    # ✅ ตรวจสอบข้อมูลสุดท้ายก่อนส่งกลับ
    print(f"📊 Final check - payment_amount non-null: {df_final['payment_amount'].notna().sum()}")
    print(f"📊 Final check - payment_amount sample values: {df_final['payment_amount'].dropna().head(5).tolist()}")
    print("\n✅ Data cleaning completed for PostgreSQL")

    return df_final

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
            # ตรวจสอบข้อมูลก่อนการทำความสะอาด
            non_null_count_before = df_clean[col].notna().sum()
            
            # แปลงเป็น string และทำความสะอาดในครั้งเดียว
            df_clean[col] = df_clean[col].astype(str)
            
            # ใช้วิธีที่เร็วขึ้น - ตรวจสอบเฉพาะค่าที่จำเป็น
            nan_mask = (
                df_clean[col].str.lower().isin(['nan', 'null', 'none', 'nonetype', 'nulltype', 'undefined']) |
                df_clean[col].str.strip().isin(['', '[null]', 'undefined']) |
                df_clean[col].str.contains('^\\[null\\]$', case=False, na=False) |
                df_clean[col].str.contains('^undefined$', case=False, na=False)
            )
            
            # แทนที่ค่า NaN string ด้วย None
            df_clean.loc[nan_mask, col] = None
            
            # ลบ whitespace เฉพาะค่าที่ไม่ใช่ None
            mask_not_none = df_clean[col].notna()
            if mask_not_none.any():
                df_clean.loc[mask_not_none, col] = df_clean.loc[mask_not_none, col].str.strip()
                
                # ตรวจสอบอีกครั้งหลังจาก strip
                nan_mask_after = (
                    df_clean[col].str.lower().isin(['nan', 'null', 'none', '', 'undefined']) |
                    df_clean[col].str.contains('^\\[null\\]$', case=False, na=False) |
                    df_clean[col].str.contains('^undefined$', case=False, na=False)
                )
                df_clean.loc[nan_mask_after, col] = None
            
            # ตรวจสอบข้อมูลหลังการทำความสะอาด
            non_null_count_after = df_clean[col].notna().sum()
            if non_null_count_before != non_null_count_after:
                print(f"⚠️ Column {col}: {non_null_count_before} → {non_null_count_after} non-null values")

    # 🔁 ล้างคอลัมน์ตัวเลขที่อาจมี comma - ใช้วิธีที่เร็วขึ้น
    numeric_cols = ['installment_amount', 'payment_amount', 'total_paid', 'late_fee']
    for col in numeric_cols:
        if col in df_clean.columns:
            # ตรวจสอบข้อมูลก่อนการทำความสะอาด
            non_null_count_before = df_clean[col].notna().sum()
            
            # ตรวจสอบว่าเป็น numeric column หรือไม่
            if df_clean[col].dtype in ['int64', 'float64']:
                # ถ้าเป็น numeric แล้ว ให้ข้ามไป
                print(f"🔍 Column {col} is already numeric, skipping conversion")
                continue
            
            # ทำความสะอาดข้อมูลก่อนลบ comma
            df_clean[col] = df_clean[col].astype(str)
            
            # ลบ comma เฉพาะค่าที่ไม่ใช่ NaN string
            mask_not_nan = ~df_clean[col].str.lower().isin(['nan', 'null', 'none', 'undefined'])
            mask_has_comma = df_clean[col].str.contains(',', na=False)
            mask_to_clean = mask_not_nan & mask_has_comma
            
            if mask_to_clean.any():
                df_clean.loc[mask_to_clean, col] = df_clean.loc[mask_to_clean, col].str.replace(',', '')
            
            # แปลงเป็น numeric เฉพาะเมื่อจำเป็น
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            # แปลง NaN เป็น None
            df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)
            
            # ตรวจสอบข้อมูลหลังการทำความสะอาด
            non_null_count_after = df_clean[col].notna().sum()
            if non_null_count_before != non_null_count_after:
                print(f"⚠️ Numeric column {col}: {non_null_count_before} → {non_null_count_after} non-null values")

    return df_clean

@op
def load_installment_data(df: pd.DataFrame):
    table_name = 'fact_installment_payments'
    pk_column = ['quotation_num', 'installment_number']

    # ✅ ตรวจสอบข้อมูลก่อนการโหลด
    print(f"🔍 Before loading - DataFrame shape: {df.shape}")
    print(f"🔍 Before loading - Columns: {list(df.columns)}")
    
    # ✅ ตรวจสอบ NaN values ก่อนการโหลด
    print("\n🔍 NaN check before loading:")
    for col in df.columns:
        nan_count = df[col].isna().sum()
        if nan_count > 0:
            print(f"  - {col}: {nan_count} NaN values")
    
    # ✅ กรองซ้ำจาก DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()
    print(f"🔍 After removing duplicates: {len(df)} rows")

    # ✅ Load ข้อมูลเดิมจาก PostgreSQL
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    print(f"📊 Existing data: {len(df_existing)} rows")

    # ✅ แปลง dtype ให้ตรงกันระหว่าง df และ df_existing
    for col in pk_column:
        if col in df.columns and col in df_existing.columns:
            # แปลงเป็น string เพื่อเปรียบเทียบ
            df[col] = df[col].astype(str)
            df_existing[col] = df_existing[col].astype(str)

    # ✅ สร้าง composite key สำหรับเปรียบเทียบ
    df['composite_key'] = df[pk_column[0]] + '|' + df[pk_column[1]]
    df_existing['composite_key'] = df_existing[pk_column[0]] + '|' + df_existing[pk_column[1]]

    # ✅ หาข้อมูลใหม่ที่ยังไม่มีในฐานข้อมูล
    existing_keys = set(df_existing['composite_key'])
    df_to_insert = df[~df['composite_key'].isin(existing_keys)].copy()

    # ✅ หาข้อมูลที่มีอยู่แล้ว และเปรียบเทียบว่าเปลี่ยนแปลงหรือไม่
    common_keys = set(df['composite_key']) & existing_keys
    df_common_new = df[df['composite_key'].isin(common_keys)].copy()
    df_common_old = df_existing[df_existing['composite_key'].isin(common_keys)].copy()

    # ตรวจสอบว่ามีข้อมูลที่ซ้ำกันหรือไม่
    if not df_common_new.empty and not df_common_old.empty:
        # ตั้ง index ด้วย composite_key
        df_common_new.set_index('composite_key', inplace=True)
        df_common_old.set_index('composite_key', inplace=True)

        # เปรียบเทียบข้อมูล (ไม่รวม pk columns และ composite_key)
        compare_cols = [col for col in df_common_new.columns if col not in pk_column + ['composite_key']]
        # ตรวจสอบว่าคอลัมน์ที่ต้องการเปรียบเทียบมีอยู่ในทั้งสอง DataFrame หรือไม่
        available_cols = [col for col in compare_cols if col in df_common_old.columns]
        
        if available_cols:
            df_common_new_compare = df_common_new[available_cols]
            df_common_old_compare = df_common_old[available_cols]

            # หาแถวที่มีการเปลี่ยนแปลง
            df_diff_mask = ~(df_common_new_compare.eq(df_common_old_compare, axis=1).all(axis=1))
            df_diff = df_common_new[df_diff_mask].reset_index()
        else:
            df_diff = pd.DataFrame()
    else:
        df_diff = pd.DataFrame()

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff)} rows")

    # ✅ Load table metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert - ใช้ Batch UPSERT เพื่อความเร็ว
    if not df_to_insert.empty:
        df_to_insert = df_to_insert.drop(columns=['composite_key'])
        
        # ✅ ตรวจสอบข้อมูลก่อนการ insert
        print(f"🔍 Before insert validation - shape: {df_to_insert.shape}")
        for col in df_to_insert.columns:
            nan_count = df_to_insert[col].isna().sum()
            if nan_count > 0:
                print(f"  - {col}: {nan_count} NaN values")
        
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna().all(axis=1)].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} rows with null primary keys")
        
        if not df_to_insert_valid.empty:
            print(f"📤 Inserting {len(df_to_insert_valid)} new records in batches...")
            
            # ใช้ batch size 1000 แถวต่อครั้ง
            batch_size = 5000
            total_batches = (len(df_to_insert_valid) + batch_size - 1) // batch_size
            
            with target_engine.begin() as conn:
                for i in range(0, len(df_to_insert_valid), batch_size):
                    batch_df = df_to_insert_valid.iloc[i:i+batch_size]
                    batch_num = (i // batch_size) + 1
                    print(f"  📦 Processing batch {batch_num}/{total_batches} ({len(batch_df)} records)")
                    
                    # ใช้ executemany สำหรับ batch insert
                    records = batch_df.to_dict(orient='records')
                    stmt = pg_insert(metadata).values(records)
                    update_columns = {
                        c.name: stmt.excluded[c.name]
                        for c in metadata.columns
                        if c.name not in pk_column
                    }
                    stmt = stmt.on_conflict_do_update(
                        index_elements=pk_column,
                        set_=update_columns
                    )
                    conn.execute(stmt)

    # ✅ Update - ใช้ Batch UPSERT เพื่อความเร็ว
    if not df_diff.empty:
        df_diff = df_diff.drop(columns=['composite_key'])
        
        # ✅ ตรวจสอบข้อมูลก่อนการ update
        print(f"🔍 Before update validation - shape: {df_diff.shape}")
        for col in df_diff.columns:
            nan_count = df_diff[col].isna().sum()
            if nan_count > 0:
                print(f"  - {col}: {nan_count} NaN values")
        
        print(f"📝 Updating {len(df_diff)} existing records in batches...")
        
        # ใช้ batch size 1000 แถวต่อครั้ง
        batch_size = 5000
        total_batches = (len(df_diff) + batch_size - 1) // batch_size
        
        with target_engine.begin() as conn:
            for i in range(0, len(df_diff), batch_size):
                batch_df = df_diff.iloc[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                print(f"  📦 Processing update batch {batch_num}/{total_batches} ({len(batch_df)} records)")
                
                # ใช้ executemany สำหรับ batch update
                records = batch_df.to_dict(orient='records')
                stmt = pg_insert(metadata).values(records)
                update_columns = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns
                    if c.name not in pk_column
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=pk_column,
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")

@job
def fact_installment_payments_etl():
    load_installment_data(clean_installment_data(extract_installment_data()))
        
if __name__ == "__main__":
    df_raw = extract_installment_data()

    df_clean = clean_installment_data((df_raw))

    # output_path = "fact_installment_payments.csv"
    # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
    # print(f"💾 Saved to {output_path}")

    load_installment_data(df_clean)
    print("🎉 completed! Data upserted to fact_installment_payments.")