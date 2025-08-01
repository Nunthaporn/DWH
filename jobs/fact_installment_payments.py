from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta

# ✅ Load env
load_dotenv()

# ✅ DB connections
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
    echo=False
)
task_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
    echo=False
)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
    echo=False,
    connect_args={
        "connect_timeout": 30,
        "application_name": "fact_installment_payments_etl",
        "options": "-c statement_timeout=300000 -c idle_in_transaction_session_timeout=300000"
    }
)

@op
def extract_installment_data():
    now = datetime.now()

    start_time = now.replace(minute=0, second=0, microsecond=0)  
    end_time = now.replace(minute=59, second=59, microsecond=999999) 

    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    try:
        print("🔄 Loading data from databases...")
        
        # ✅ เพิ่ม WHERE clause เพื่อโหลดเฉพาะข้อมูลที่จำเป็น
        df_plan = pd.read_sql(f"""
            SELECT quo_num
            FROM fin_system_select_plan
            WHERE datestart BETWEEN '{start_str}' AND '{end_str}'
            AND type_insure IN ('ประกันรถ', 'ตรอ')
        """, source_engine)

        # ✅ โหลดเฉพาะข้อมูลที่มี quo_num ใน df_plan
        if not df_plan.empty:
            quo_nums = "','".join(df_plan['quo_num'].dropna().astype(str))
            df_installment = pd.read_sql(f"""
                SELECT quo_num, money_one, money_two, money_three, money_four,
                       money_five, money_six, money_seven, money_eight, money_nine,
                       money_ten, money_eleven, money_twelve,
                       date_one, date_two, date_three, date_four, date_five,
                       date_six, date_seven, date_eight, date_nine, date_ten,
                       date_eleven, date_twelve, numpay
                FROM fin_installment
                WHERE quo_num IN ('{quo_nums}')
            """, source_engine)
        else:
            df_installment = pd.DataFrame()

        df_order = pd.read_sql("""
            SELECT quo_num, order_number
            FROM fin_order
            WHERE type_insure IN ('ประกันรถ', 'ตรอ')
        """, task_engine)

        # ✅ โหลดเฉพาะข้อมูลที่มี order_number ใน df_order
        if not df_order.empty:
            order_nums = "','".join(df_order['order_number'].dropna().astype(str))
            df_finance = pd.read_sql(f"""
                SELECT order_number, datepay_one, datepay_two, datepay_three, datepay_four,
                       datepay_five, datepay_six, datepay_seven, datepay_eight,
                       datepay_nine, datepay_ten, datepay_eleven, datepay_twelve,
                       moneypay_one, moneypay_two, moneypay_three, moneypay_four,
                       moneypay_five, moneypay_six, moneypay_seven, moneypay_eight,
                       moneypay_nine, moneypay_ten, moneypay_eleven, moneypay_twelve,
                       numpay
                FROM fin_finance
                WHERE order_number IN ('{order_nums}')
            """, task_engine)

            df_bill = pd.read_sql(f"""
                SELECT order_number, bill_receipt, bill_receipt2, bill_receipt3,
                       bill_receipt4, bill_receipt5, bill_receipt6, bill_receipt7,
                       bill_receipt8, bill_receipt9, bill_receipt10, bill_receipt11, bill_receipt12
                FROM fin_bill
                WHERE order_number IN ('{order_nums}')
            """, task_engine)
        else:
            df_finance = pd.DataFrame()
            df_bill = pd.DataFrame()

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
        
        # ✅ ลด memory usage โดยการลบข้อมูลที่ไม่จำเป็น
        import gc
        gc.collect()
        
    except Exception as e:
        print(f"❌ Error during data extraction: {e}")
        # ✅ Rollback connections เพื่อแก้ปัญหา PendingRollbackError
        for engine_name, engine in [("source", source_engine), ("task", task_engine), ("target", target_engine)]:
            try:
                with engine.connect() as conn:
                    conn.rollback()
                print(f"✅ Rollback successful for {engine_name} engine")
            except Exception as rollback_error:
                print(f"⚠️ Rollback failed for {engine_name} engine: {rollback_error}")
        
        # ✅ ปิด connections ทั้งหมด
        try:
            source_engine.dispose()
            task_engine.dispose()
            target_engine.dispose()
            print("✅ All engines disposed successfully")
        except Exception as dispose_error:
            print(f"⚠️ Engine disposal failed: {dispose_error}")
        
        raise e

    # ✅ ลด debug prints เหลือแค่ข้อมูลสำคัญ
    print(f"📦 Data loaded: plan({df_plan.shape[0]}), installment({df_installment.shape[0]}), "
          f"order({df_order.shape[0]}), finance({df_finance.shape[0]}), "
          f"bill({df_bill.shape[0]}), late_fee({df_late_fee.shape[0]}), test({df_test.shape[0]})")

    return df_plan, df_installment, df_order, df_finance, df_bill, df_late_fee, df_test

@op
def clean_installment_data(inputs):
    df_plan, df_inst, df_order, df_fin, df_bill, df_fee, df_test = inputs

    print("🔄 Processing installment data...")

    # 1. เตรียมข้อมูลค่างวด + วันที่
    df_inst['numpay'] = pd.to_numeric(df_inst['numpay'], errors='coerce')
    df_filtered = df_inst[df_inst['numpay'].notna() & (df_inst['numpay'] > 0)]

    money_cols = [f'money_{n}' for n in ['one', 'two', 'three', 'four', 'five', 'six',
                                         'seven', 'eight', 'nine', 'ten', 'eleven', 'twelve']]
    date_cols = [f'date_{n}' for n in ['one', 'two', 'three', 'four', 'five', 'six',
                                       'seven', 'eight', 'nine', 'ten', 'eleven', 'twelve']]

    # ✅ ลด debug prints เหลือแค่ข้อมูลสำคัญ
    print(f"📊 Processing {df_filtered.shape[0]} installment records")

    df_money = df_filtered.melt(id_vars=['quo_num', 'numpay'], value_vars=money_cols,
                                 var_name='installment_period', value_name='installment_amount')
    df_date = df_filtered.melt(id_vars=['quo_num', 'numpay'], value_vars=date_cols,
                                var_name='due_date_period', value_name='due_date')

    df_combined = pd.concat([df_money.reset_index(drop=True), df_date['due_date']], axis=1)
    df_combined['installment_number'] = df_combined.groupby('quo_num').cumcount() + 1
    df_combined = df_combined[df_combined['installment_number'] <= df_combined['numpay']]
    df_combined = df_combined.sort_values(by=['quo_num', 'due_date'])
    df_combined['installment_number'] = df_combined.groupby('quo_num').cumcount() + 1

    print(f"📊 Combined {df_combined.shape[0]} installment records")

    # 2. ผูก order_number
    df_join = pd.merge(df_combined, df_order, on='quo_num', how='left')
    print(f"📊 Joined with orders: {df_join.shape[0]} records")

    # 3. เตรียมข้อมูลการชำระ
    num_to_name = ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight',
                   'nine', 'ten', 'eleven', 'twelve']
    
    df_fin['numpay'] = pd.to_numeric(df_fin['numpay'], errors='coerce')
    df_fin = df_fin[df_fin['numpay'].notna() & (df_fin['numpay'] > 0)]

    print(f"📊 Processing {df_fin.shape[0]} finance records")

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
    print(f"📊 Created {df_payment.shape[0]} payment records")

    # 4. เตรียม payment proof
    df_proof = df_bill.melt(id_vars=['order_number'],
        value_vars=[f'bill_receipt{i if i > 1 else ""}' for i in range(1, 13)],
        value_name='payment_proof')
    df_proof = df_proof[df_proof['payment_proof'].notna()].reset_index()
    df_proof['installment_number'] = df_proof.groupby('order_number').cumcount() + 1
    df_proof = df_proof[['order_number', 'installment_number', 'payment_proof']]

    print(f"📊 Created {df_proof.shape[0]} proof records")

    # 5. รวมทั้งหมด
    df = pd.merge(df_join, df_payment, on=['order_number', 'installment_number'], how='left')
    df = pd.merge(df, df_proof, on=['order_number', 'installment_number'], how='left')

    print(f"📊 Final merged data: {df.shape[0]} records")

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
    # ✅ ลบ comma ออกจากข้อมูลก่อนแปลงเป็น numeric
    if df['payment_amount'].dtype == 'object':
        # ทำความสะอาดข้อมูลก่อนลบ comma
        df['payment_amount'] = df['payment_amount'].astype(str)
        
        # ลบ comma เฉพาะค่าที่ไม่ใช่ NaN string
        mask_not_nan = ~df['payment_amount'].str.lower().isin(['nan', 'null', 'none', 'undefined'])
        df.loc[mask_not_nan, 'payment_amount'] = df.loc[mask_not_nan, 'payment_amount'].str.replace(',', '')
    
    if df['installment_amount'].dtype == 'object':
        # ทำความสะอาดข้อมูลก่อนลบ comma
        df['installment_amount'] = df['installment_amount'].astype(str)
        
        # ลบ comma เฉพาะค่าที่ไม่ใช่ NaN string
        mask_not_nan = ~df['installment_amount'].str.lower().isin(['nan', 'null', 'none', 'undefined'])
        df.loc[mask_not_nan, 'installment_amount'] = df.loc[mask_not_nan, 'installment_amount'].str.replace(',', '')
    
    # ✅ แปลงเป็น numeric และจัดการ NaN
    df['payment_amount'] = pd.to_numeric(df['payment_amount'], errors='coerce')
    df['payment_amount'] = df['payment_amount'].where(pd.notna(df['payment_amount']), None)
    
    df['installment_amount'] = pd.to_numeric(df['installment_amount'], errors='coerce')
    df['late_fee'] = pd.to_numeric(df['late_fee'], errors='coerce').fillna(0)
    
    # ✅ แปลง NaN เป็น None
    df['installment_amount'] = df['installment_amount'].where(pd.notna(df['installment_amount']), None)
    df['late_fee'] = df['late_fee'].where(pd.notna(df['late_fee']), None)
    
    # ✅ ตรวจสอบค่าที่เป็น Infinity หรือ -Infinity
    df['payment_amount'] = df['payment_amount'].replace([np.inf, -np.inf], np.nan)
    df['installment_amount'] = df['installment_amount'].replace([np.inf, -np.inf], np.nan)
    df['late_fee'] = df['late_fee'].replace([np.inf, -np.inf], 0)
    
    # ✅ แปลง NaN เป็น None หลังจากจัดการ Infinity
    df['payment_amount'] = df['payment_amount'].where(pd.notna(df['payment_amount']), None)
    df['installment_amount'] = df['installment_amount'].where(pd.notna(df['installment_amount']), None)
    df['late_fee'] = df['late_fee'].where(pd.notna(df['late_fee']), None)
    
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
    
    # ✅ แปลง NaN เป็น None สำหรับ total_paid
    df['total_paid'] = df['total_paid'].where(pd.notna(df['total_paid']), None)

    print(f"📊 Calculated total_paid for {df['total_paid'].notna().sum()} records")

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
    
    # ✅ แปลง NaN เป็น None สำหรับ installment_number
    df['installment_number'] = df['installment_number'].where(pd.notna(df['installment_number']), None)
    
    # ✅ แปลงวันที่อย่างปลอดภัย
    df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')
    df['due_date'] = df['due_date'].dt.strftime('%Y%m%d')
    df['due_date'] = df['due_date'].where(pd.notna(df['due_date']), None)
    
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
    df['payment_date'] = df['payment_date'].dt.strftime('%Y%m%d')
    df['payment_date'] = df['payment_date'].where(pd.notna(df['payment_date']), None)
    
    # ✅ ใช้ sanitize_dataframe function เพื่อทำความสะอาดข้อมูลอย่างครอบคลุม
    df = sanitize_dataframe(df.copy())

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
    print(f"📊 Final data ready: {df_final.shape[0]} records")
    
    # ✅ แปลง NaN string เป็น None สำหรับ PostgreSQL
    for col in df_final.columns:
        if df_final[col].dtype == 'object':
            df_final[col] = df_final[col].replace(['nan', 'null', 'none', 'undefined', 'NaN', 'NULL', 'NONE', 'UNDEFINED'], None)
    
    # ✅ ทำความสะอาดข้อมูลขั้นสุดท้าย - แปลง NaN เป็น None สำหรับทุกคอลัมน์
    for col in df_final.columns:
        if df_final[col].dtype in ['float64', 'int64']:
            # แปลง NaN เป็น None สำหรับ numeric columns
            df_final[col] = df_final[col].where(pd.notna(df_final[col]), None)
        elif df_final[col].dtype == 'object':
            # แปลง string 'nan', 'null' เป็น None
            df_final[col] = df_final[col].astype(str)
            nan_mask = df_final[col].str.lower().isin(['nan', 'null', 'none', 'undefined'])
            df_final.loc[nan_mask, col] = None

    print("✅ Data cleaning completed for PostgreSQL")

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
                # ถ้าเป็น numeric แล้ว ให้แปลง NaN เป็น None
                df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)
                print(f"🔍 Column {col} is numeric, converted NaN to None")
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

def clean_records_for_db(records: list) -> list:
    """ทำความสะอาด records ก่อนส่งไปยังฐานข้อมูล"""
    cleaned_records = []
    
    for i, record in enumerate(records):
        cleaned_record = {}
        for key, value in record.items():
            # ตรวจสอบและทำความสะอาดค่า
            if pd.isna(value):
                cleaned_record[key] = None
            elif isinstance(value, str):
                if value.lower() in ['nan', 'null', 'none', 'undefined', '']:
                    cleaned_record[key] = None
                else:
                    cleaned_record[key] = value.strip()
            elif isinstance(value, (int, float)):
                if pd.isna(value) or value in [np.inf, -np.inf]:
                    cleaned_record[key] = None
                else:
                    cleaned_record[key] = value
            else:
                cleaned_record[key] = value
        
        cleaned_records.append(cleaned_record)
        
        # แสดงตัวอย่างการทำความสะอาดสำหรับ 3 records แรก
        if i < 3:
            print(f"    🔍 Record {i} cleaned: {cleaned_record}")
    
    return cleaned_records

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
    
    # ✅ ทำความสะอาดข้อมูลก่อนโหลด - แปลง NaN เป็น None
    print("\n🧹 Final data cleaning before loading...")
    df_clean = df.copy()
    for col in df_clean.columns:
        if df_clean[col].dtype in ['float64', 'int64']:
            # แปลง NaN เป็น None สำหรับ numeric columns
            nan_count = df_clean[col].isna().sum()
            if nan_count > 0:
                print(f"  🔄 Converting {nan_count} NaN values to None in {col}")
                df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)
        elif df_clean[col].dtype == 'object':
            # แปลง string 'nan', 'null' เป็น None
            df_clean[col] = df_clean[col].astype(str)
            nan_mask = df_clean[col].str.lower().isin(['nan', 'null', 'none', 'undefined'])
            nan_count = nan_mask.sum()
            if nan_count > 0:
                print(f"  🔄 Converting {nan_count} string NaN values to None in {col}")
                df_clean.loc[nan_mask, col] = None
    
    # ✅ ตรวจสอบข้อมูลหลังการทำความสะอาด
    print("\n🔍 NaN check after cleaning:")
    for col in df_clean.columns:
        nan_count = df_clean[col].isna().sum()
        if nan_count > 0:
            print(f"  - {col}: {nan_count} NaN values")
    
    # ใช้ df_clean แทน df ต่อไป
    df = df_clean
    
    # ✅ กรองซ้ำจาก DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()
    print(f"🔍 After removing duplicates: {len(df)} rows")

    # ✅ วันปัจจุบัน (เริ่มต้นเวลา 00:00:00)
    today_str = datetime.now().strftime('%Y-%m-%d')

    # ✅ Load เฉพาะข้อมูลวันนี้จาก PostgreSQL - เพิ่มการจัดการ connection
    df_existing = pd.DataFrame()
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"🔄 Attempting to load existing data (attempt {retry_count + 1}/{max_retries})...")
            
            # สร้าง connection ใหม่ทุกครั้ง
            with target_engine.connect() as conn:
                # ตั้งค่า timeout และ connection parameters
                conn.execute("SET statement_timeout = 300000")  # 5 minutes
                conn.execute("SET idle_in_transaction_session_timeout = 300000")  # 5 minutes
                
                df_existing = pd.read_sql(
                    f"SELECT * FROM {table_name} WHERE update_at >= '{today_str}'",
                    conn
                )
            
            print(f"📊 Existing data loaded successfully: {len(df_existing)} rows")
            break
            
        except Exception as e:
            retry_count += 1
            print(f"❌ Error loading existing data (attempt {retry_count}/{max_retries}): {e}")
            
            if retry_count >= max_retries:
                print("⚠️ Max retries reached. Proceeding without existing data comparison.")
                df_existing = pd.DataFrame()
                break
            
            # รอสักครู่ก่อนลองใหม่
            time.sleep(2 ** retry_count)  # Exponential backoff

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
        
        # ✅ ทำความสะอาดข้อมูลก่อน insert - แปลง NaN เป็น None
        for col in df_to_insert.columns:
            if df_to_insert[col].dtype in ['float64', 'int64']:
                df_to_insert[col] = df_to_insert[col].where(pd.notna(df_to_insert[col]), None)
            elif df_to_insert[col].dtype == 'object':
                df_to_insert[col] = df_to_insert[col].astype(str)
                nan_mask = df_to_insert[col].str.lower().isin(['nan', 'null', 'none', 'undefined'])
                df_to_insert.loc[nan_mask, col] = None
        
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna().all(axis=1)].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} rows with null primary keys")
        
        if not df_to_insert_valid.empty:
            print(f"📤 Inserting {len(df_to_insert_valid)} new records...")
            
            # ใช้ batch size เล็กลงเพื่อลดปัญหา connection
            batch_size = 1000
            total_batches = (len(df_to_insert_valid) + batch_size - 1) // batch_size
            
            # ✅ ใช้ connection แยกสำหรับแต่ละ batch
            for i in range(0, len(df_to_insert_valid), batch_size):
                batch_df = df_to_insert_valid.iloc[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                print(f"  📦 Processing batch {batch_num}/{total_batches} ({len(batch_df)} records)")
                
                # ใช้ executemany สำหรับ batch insert
                records = batch_df.to_dict(orient='records')
                
                # ✅ ทำความสะอาด records ก่อนส่งไปยังฐานข้อมูล
                records = clean_records_for_db(records)
                
                # ✅ ใช้ connection แยกสำหรับแต่ละ batch พร้อม retry logic
                max_retries = 3
                retry_count = 0
                
                while retry_count < max_retries:
                    try:
                        with target_engine.begin() as conn:
                            # ตั้งค่า timeout
                            conn.execute("SET statement_timeout = 300000")  # 5 minutes
                            conn.execute("SET idle_in_transaction_session_timeout = 300000")  # 5 minutes
                            
                            stmt = pg_insert(metadata).values(records)
                            update_columns = {
                                c.name: stmt.excluded[c.name]
                                for c in metadata.columns
                                if c.name not in pk_column
                            }
                            update_columns["update_at"] = datetime.now()  # ✅ เพิ่มให้ update timestamp ทุกครั้ง

                            stmt = stmt.on_conflict_do_update(
                                index_elements=pk_column,
                                set_=update_columns
                            )
                            conn.execute(stmt)
                        
                        print(f"    ✅ Batch {batch_num} inserted successfully")
                        break
                        
                    except Exception as e:
                        retry_count += 1
                        print(f"    ❌ Error inserting batch {batch_num} (attempt {retry_count}/{max_retries}): {e}")
                        
                        if retry_count >= max_retries:
                            print(f"    ⚠️ Max retries reached for batch {batch_num}. Skipping this batch.")
                            break
                        
                        # รอสักครู่ก่อนลองใหม่
                        time.sleep(2 ** retry_count)  # Exponential backoff

    # ✅ Update - ใช้ Batch UPSERT เพื่อความเร็ว
    if not df_diff.empty:
        df_diff = df_diff.drop(columns=['composite_key'])
        
        # ✅ ทำความสะอาดข้อมูลก่อน update - แปลง NaN เป็น None
        for col in df_diff.columns:
            if df_diff[col].dtype in ['float64', 'int64']:
                df_diff[col] = df_diff[col].where(pd.notna(df_diff[col]), None)
            elif df_diff[col].dtype == 'object':
                df_diff[col] = df_diff[col].astype(str)
                nan_mask = df_diff[col].str.lower().isin(['nan', 'null', 'none', 'undefined'])
                df_diff.loc[nan_mask, col] = None
        
        print(f"📝 Updating {len(df_diff)} existing records...")
        
        # ใช้ batch size เล็กลงเพื่อลดปัญหา connection
        batch_size = 1000
        total_batches = (len(df_diff) + batch_size - 1) // batch_size
        
        # ✅ ใช้ connection แยกสำหรับแต่ละ batch
        for i in range(0, len(df_diff), batch_size):
            batch_df = df_diff.iloc[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            print(f"  📦 Processing update batch {batch_num}/{total_batches} ({len(batch_df)} records)")
            
            # ใช้ executemany สำหรับ batch update
            records = batch_df.to_dict(orient='records')
            
            # ✅ ทำความสะอาด records ก่อนส่งไปยังฐานข้อมูล
            records = clean_records_for_db(records)
            
            # ✅ ใช้ connection แยกสำหรับแต่ละ batch พร้อม retry logic
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    with target_engine.begin() as conn:
                        # ตั้งค่า timeout
                        conn.execute("SET statement_timeout = 300000")  # 5 minutes
                        conn.execute("SET idle_in_transaction_session_timeout = 300000")  # 5 minutes
                        
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
                    
                    print(f"    ✅ Update batch {batch_num} completed successfully")
                    break
                    
                except Exception as e:
                    retry_count += 1
                    print(f"    ❌ Error updating batch {batch_num} (attempt {retry_count}/{max_retries}): {e}")
                    
                    if retry_count >= max_retries:
                        print(f"    ⚠️ Max retries reached for update batch {batch_num}. Skipping this batch.")
                        break
                    
                    # รอสักครู่ก่อนลองใหม่
                    time.sleep(2 ** retry_count)  # Exponential backoff

    print("✅ Insert/update completed.")
    
    # ✅ ปิด connections หลังจากเสร็จสิ้น
    try:
        source_engine.dispose()
        task_engine.dispose()
        target_engine.dispose()
        print("✅ All database connections closed successfully")
    except Exception as e:
        print(f"⚠️ Error closing database connections: {e}")

@job
def fact_installment_payments_etl():
    load_installment_data(clean_installment_data(extract_installment_data()))
        
# if __name__ == "__main__":
#     df_raw = extract_installment_data()

#     df_clean = clean_installment_data((df_raw))

#     output_path = "fact_installment_payments.csv"
#     df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
#     print(f"💾 Saved to {output_path}")

#     load_installment_data(df_clean)
#     print("🎉 completed! Data upserted to fact_installment_payments.")