from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
import time
import gc
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, inspect, text, func, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql.elements import TextClause
from typing import Union, List
from sqlalchemy import or_, func

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

def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def load_query_raw(engine, query: str) -> pd.DataFrame:
    raw_conn = None
    try:
        raw_conn = engine.raw_connection()
        cursor = raw_conn.cursor()
        cursor.execute(query)
        return pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    except Exception as e:
        print(f"❌ Error loading query: {e}")
        return pd.DataFrame()
    finally:
        if raw_conn:
            try: raw_conn.close()
            except Exception as e: print(f"⚠️ Error closing: {e}")
        engine.dispose()

def load_data_in_batches(engine, table_name: str, order_numbers: List[str], batch_size=10000) -> pd.DataFrame:
    all_results = []
    for batch in chunk_list(order_numbers, batch_size):
        formatted = "', '".join(batch)
        query = f"SELECT * FROM {table_name} WHERE order_number IN ('{formatted}')"
        df_batch = load_query_raw(engine, query)
        all_results.append(df_batch)
    return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

@op
def extract_installment_data():
    from sqlalchemy import create_engine

    def create_fresh_engine(engine_url, engine_name):
        try:
            return create_engine(
                engine_url,
                pool_size=1,
                max_overflow=2,
                pool_timeout=30,
                pool_recycle=300,
                pool_pre_ping=True,
                echo=False,
                connect_args={
                    'connect_timeout': 60,
                    'autocommit': False,
                    'charset': 'utf8mb4',
                    'sql_mode': 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'
                }
            )
        except Exception as e:
            print(f"❌ Error creating {engine_name} engine: {e}")
            return None

    source_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
    task_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task"

    print("📊 Loading plan data...")
    fresh_source_1 = create_fresh_engine(source_url, "source_1")
    if fresh_source_1:
        try:
            df_plan = pd.read_sql("""
                SELECT quo_num
                FROM fin_system_select_plan
                WHERE datestart BETWEEN '2025-01-01' AND '2025-09-08'
            """, fresh_source_1)
        except Exception as e:
            print(f"❌ Error loading plan data: {e}")
            df_plan = pd.DataFrame()
        finally:
            fresh_source_1.dispose()
    else:
        df_plan = pd.DataFrame()

    df_installment = pd.DataFrame()
    if not df_plan.empty:
        print("📊 Loading installment data...")
        fresh_source_2 = create_fresh_engine(source_url, "source_2")
        if fresh_source_2:
            try:
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
                """, fresh_source_2)
            except Exception as e:
                print(f"❌ Error loading installment data: {e}")
                df_installment = pd.DataFrame()
            finally:
                fresh_source_2.dispose()

    print("📊 Loading order data...")
    df_order = pd.DataFrame()
    fresh_task_1 = create_fresh_engine(task_url, "task_1")
    if fresh_task_1:
        try:
            with fresh_task_1.begin() as conn:
                df_order = pd.read_sql("""
                    SELECT quo_num, order_number
                    FROM fin_order
                """, conn)
                print(f"✅ Loaded {len(df_order)} order records")
        except Exception as e:
            print(f"❌ Error loading order data: {e}")
        finally:
            fresh_task_1.dispose()

    order_list = list(df_order["order_number"].dropna().astype(str).unique())
    print(f"📦 Loading bill data in batches... ({len(order_list)} orders)")

    df_finance = pd.DataFrame()
    if not df_order.empty:
        print("📊 Loading finance and bill data...")
        task_engine_finance = create_fresh_engine(task_url, "task_engine_finance")
        df_finance = load_data_in_batches(task_engine_finance, "fin_finance", order_list)
        print(f"✅ Loaded {len(df_finance)} finance records")

    print("📊 Loading bill data...")
    df_bill = pd.DataFrame()
    task_engine_bill = create_fresh_engine(task_url, "task_engine_bill")
    if task_engine_bill:
        try:
            df_bill = load_data_in_batches(task_engine_bill, "fin_bill", order_list)
            print(f"✅ Loaded {len(df_bill)} bill records")
        except Exception as e:
            print(f"❌ Error loading bill data: {e}")
        finally:
            task_engine_bill.dispose()

    print("📊 Loading late fee data...")
    df_late_fee = pd.DataFrame()
    fresh_task_3 = create_fresh_engine(task_url, "task_3")
    if fresh_task_3:
        try:
            with fresh_task_3.begin() as conn:
                df_late_fee = pd.read_sql("""
                    SELECT orderNumber, penaltyPay, numPay
                    FROM FIN_Account_AttachSlip_PathImageSlip
                    WHERE checkPay IN ('ค่าปรับ', 'ค่างวด/ค่าปรับ')
                """, conn)
                print(f"✅ Loaded {len(df_late_fee)} late fee records")
        except Exception as e:
            print(f"❌ Error loading late fee data: {e}")
        finally:
            fresh_task_3.dispose()

    print("📊 Loading test data...")
    df_test = pd.DataFrame()
    fresh_source_3 = create_fresh_engine(source_url, "source_3")
    if fresh_source_3:
        try:
            with fresh_source_3.begin() as conn:
                df_test = pd.read_sql("""
                    SELECT quo_num
                    FROM fin_system_select_plan
                    WHERE name IN ('ทดสอบ', 'test')
                """, conn)
                print(f"✅ Loaded {len(df_test)} test records")
        except Exception as e:
            print(f"❌ Error loading test data: {e}")
        finally:
            fresh_source_3.dispose()

    print(f"📦 Data loaded: plan({df_plan.shape[0]}), installment({df_installment.shape[0]}), order({df_order.shape[0]}), finance({df_finance.shape[0]}), bill({df_bill.shape[0]}), late_fee({df_late_fee.shape[0]}), test({df_test.shape[0]})")

    return df_plan, df_installment, df_order, df_finance, df_bill, df_late_fee, df_test

@op
def clean_installment_data(inputs):
    df_plan, df_inst, df_order, df_fin, df_bill, df_fee, df_test = inputs

    print("🔄 Processing installment data...")
    
    # ✅ ตรวจสอบข้อมูลที่ได้รับ
    print(f"📊 Input data summary:")
    print(f"  - Plan: {df_plan.shape[0]} records, columns: {list(df_plan.columns) if not df_plan.empty else 'empty'}")
    print(f"  - Installment: {df_inst.shape[0]} records, columns: {list(df_inst.columns) if not df_inst.empty else 'empty'}")
    print(f"  - Order: {df_order.shape[0]} records, columns: {list(df_order.columns) if not df_order.empty else 'empty'}")
    print(f"  - Finance: {df_fin.shape[0]} records, columns: {list(df_fin.columns) if not df_fin.empty else 'empty'}")
    print(f"  - Bill: {df_bill.shape[0]} records, columns: {list(df_bill.columns) if not df_bill.empty else 'empty'}")
    print(f"  - Late fee: {df_fee.shape[0]} records, columns: {list(df_fee.columns) if not df_fee.empty else 'empty'}")
    print(f"  - Test: {df_test.shape[0]} records, columns: {list(df_test.columns) if not df_test.empty else 'empty'}")

    # 1. เตรียมข้อมูลค่างวด + วันที่
    if df_inst.empty:
        print("⚠️ Installment DataFrame is empty. Creating empty combined records.")
        df_combined = pd.DataFrame(columns=['quo_num', 'numpay', 'installment_period', 'installment_amount', 'due_date', 'installment_number'])
    elif 'numpay' not in df_inst.columns:
        print("⚠️ Installment DataFrame does not have 'numpay' column. Creating empty combined records.")
        df_combined = pd.DataFrame(columns=['quo_num', 'numpay', 'installment_period', 'installment_amount', 'due_date', 'installment_number'])
    elif 'quo_num' not in df_inst.columns:
        print("⚠️ Installment DataFrame does not have 'quo_num' column. Creating empty combined records.")
        df_combined = pd.DataFrame(columns=['quo_num', 'numpay', 'installment_period', 'installment_amount', 'due_date', 'installment_number'])
    else:
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
    if df_order.empty:
        print("⚠️ Order DataFrame is empty. Creating empty joined records.")
        df_join = df_combined.copy()
        df_join['order_number'] = None
    elif 'quo_num' not in df_order.columns:
        print("⚠️ Order DataFrame does not have 'quo_num' column. Creating empty joined records.")
        df_join = df_combined.copy()
        df_join['order_number'] = None
    elif 'order_number' not in df_order.columns:
        print("⚠️ Order DataFrame does not have 'order_number' column. Creating empty joined records.")
        df_join = df_combined.copy()
        df_join['order_number'] = None
    else:
        df_join = pd.merge(df_combined, df_order, on='quo_num', how='left')
        print(f"📊 Joined with orders: {df_join.shape[0]} records")

    # 3. เตรียมข้อมูลการชำระ
    num_to_name = ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight',
                   'nine', 'ten', 'eleven', 'twelve']
    
    # ✅ ตรวจสอบว่า df_fin มีข้อมูลและมีคอลัมน์ที่จำเป็นหรือไม่
    if df_fin.empty:
        print("⚠️ Finance DataFrame is empty. Creating empty payment records.")
        df_payment = pd.DataFrame(columns=['order_number', 'payment_amount', 'payment_date', 'installment_number'])
    elif 'numpay' not in df_fin.columns:
        print("⚠️ Finance DataFrame does not have 'numpay' column. Creating empty payment records.")
        df_payment = pd.DataFrame(columns=['order_number', 'payment_amount', 'payment_date', 'installment_number'])
    elif 'order_number' not in df_fin.columns:
        print("⚠️ Finance DataFrame does not have 'order_number' column. Creating empty payment records.")
        df_payment = pd.DataFrame(columns=['order_number', 'payment_amount', 'payment_date', 'installment_number'])
    else:
        df_fin['numpay'] = pd.to_numeric(df_fin['numpay'], errors='coerce')
        df_fin = df_fin[df_fin['numpay'].notna() & (df_fin['numpay'] > 0)]

        print(f"📊 Processing {df_fin.shape[0]} finance records")

        # ใช้ vectorized operations แทน iterrows()
        rows_list = []
        for i, sfx in enumerate(num_to_name, start=1):
            money_col = f'moneypay_{sfx}'
            date_col = f'datepay_{sfx}'
            
            # ✅ ตรวจสอบว่าคอลัมน์ที่จำเป็นมีอยู่หรือไม่
            if money_col not in df_fin.columns or date_col not in df_fin.columns:
                print(f"⚠️ Missing columns {money_col} or {date_col} in finance data. Skipping installment {i}.")
                continue
            
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
    if df_bill.empty:
        print("⚠️ Bill DataFrame is empty. Creating empty proof records.")
        df_proof = pd.DataFrame(columns=['order_number', 'installment_number', 'payment_proof'])
    elif 'order_number' not in df_bill.columns:
        print("⚠️ Bill DataFrame does not have 'order_number' column. Creating empty proof records.")
        df_proof = pd.DataFrame(columns=['order_number', 'installment_number', 'payment_proof'])
    else:
        # ✅ ตรวจสอบว่ามีคอลัมน์ bill_receipt หรือไม่
        bill_receipt_cols = [f'bill_receipt{i if i > 1 else ""}' for i in range(1, 13)]
        missing_cols = [col for col in bill_receipt_cols if col not in df_bill.columns]
        if missing_cols:
            print(f"⚠️ Bill DataFrame missing columns: {missing_cols}. Creating empty proof records.")
            df_proof = pd.DataFrame(columns=['order_number', 'installment_number', 'payment_proof'])
        else:
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
    if df_fee.empty:
        print("⚠️ Late fee DataFrame is empty. Adding empty late_fee column.")
        df['late_fee'] = 0
    elif 'orderNumber' not in df_fee.columns or 'penaltyPay' not in df_fee.columns or 'numPay' not in df_fee.columns:
        print("⚠️ Late fee DataFrame missing required columns. Adding empty late_fee column.")
        df['late_fee'] = 0
    else:
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
    if not df_test.empty and 'quo_num' in df_test.columns:
        df = df[~df['quo_num'].isin(df_test['quo_num'])]
        print(f"📊 Filtered out {len(df_test)} test records")
    else:
        print("⚠️ Test DataFrame is empty or missing 'quo_num' column. Skipping test record filtering.")
    
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

def create_fresh_target_engine():
    """สร้าง fresh target engine สำหรับ PostgreSQL"""
    target_url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:"
        f"{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:"
        f"{os.getenv('DB_PORT_test')}/fininsurance"
    )
    try:
        return create_engine(
            target_url,
            pool_size=5,
            max_overflow=10,
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
    except Exception as e:
        print(f"❌ Error creating fresh target engine: {e}")
        return None


def chunker(df: pd.DataFrame, size: int = 5000):
    for i in range(0, len(df), size):
        yield df.iloc[i:i+size]


def clean_records_for_db(records: list[dict]) -> list[dict]:
    """แปลง NaN/NaT และ numpy scalar ให้เป็นชนิด Python + None ที่ DB รับได้"""
    def to_native(v):
        if v is None:
            return None
        if isinstance(v, float) and (np.isnan(v) if isinstance(v, float) else False):
            return None
        if pd.isna(v):
            return None
        # numpy scalar → python
        if isinstance(v, (np.integer, )):
            return int(v)
        if isinstance(v, (np.floating, )):
            return float(v)
        if isinstance(v, (np.bool_, )):
            return bool(v)
        return v

    out = []
    for r in records:
        out.append({k: to_native(v) for k, v in r.items()})
    return out


def build_conditional_upsert(table, pk_cols: list[str], update_allow_cols: list[str], records: list[dict]):
    """
    UPSERT:
      - INSERT แถวใหม่
      - UPDATE เฉพาะเมื่อค่า 'ต่างจริง ๆ' (NULL-safe) ด้วย IS DISTINCT FROM
      - ไม่ทับด้วย NULL: COALESCE(EXCLUDED.col, table.col)
      - update_at = now() เฉพาะตอน UPDATE จริง (เพราะมี WHERE)
    """
    stmt = pg_insert(table).values(records)
    excluded = stmt.excluded

    # อัปเดตด้วยค่าใหม่ถ้าไม่ใช่ NULL มิฉะนั้นคงค่าเดิม
    set_map = {
        col: func.coalesce(getattr(excluded, col), getattr(table.c, col))
        for col in update_allow_cols
    }
    if 'update_at' in table.c:
        set_map['update_at'] = func.now()

    # อัปเดตเฉพาะเมื่อ "ค่าหลัง COALESCE" ต่างจากค่าปัจจุบัน (NULL-safe)
    change_conds = [
        func.coalesce(getattr(excluded, col), getattr(table.c, col))\
            .is_distinct_from(getattr(table.c, col))
        for col in update_allow_cols
    ]
    where_clause = or_(*change_conds) if change_conds else text("FALSE")

    return stmt.on_conflict_do_update(
        index_elements=pk_cols,  # หรือใช้ constraint="fact_installment_payments_unique" ถ้าต้องระบุชื่อคอนสเตรนต์
        set_=set_map,
        where=where_clause
    )

# ----------------------- Main OP -----------------------

@op
def load_installment_data(df: pd.DataFrame):
    table_name = 'fact_installment_payments'
    pk_column = ['quotation_num', 'installment_number']

    # ✅ ตรวจสอบข้อมูลก่อนการโหลด
    print(f"🔍 Before loading - DataFrame shape: {df.shape}")
    print(f"🔍 Before loading - Columns: {list(df.columns)}")

    # ✅ ตรวจสอบ NaN values ก่อนการโหลด (log สั้น ๆ)
    print("\n🔍 NaN check before loading:")
    for col in df.columns:
        nan_count = df[col].isna().sum()
        if nan_count > 0:
            print(f"  - {col}: {nan_count} NaN values")

    # ✅ ทำความสะอาดข้อมูลพื้นฐาน
    print("\n🧹 Final data cleaning before loading...")
    df_clean = df.copy()

    # แปลง object 'nan'/'null'/'none'/'undefined' → None
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            s = df_clean[col].astype(str)
            mask = s.str.lower().isin(['nan', 'null', 'none', 'undefined'])
            if mask.any():
                print(f"  🔄 Converting {mask.sum()} string NaN-like values to None in {col}")
                df_clean.loc[mask, col] = None

    # กรองแถวที่คีย์ผสมว่าง
    # แปลงชนิดให้เทียบง่าย และคัดทิ้งคีย์ที่เป็น NaN/None
    for col in pk_column:
        if col not in df_clean.columns:
            print(f"❌ Missing primary key column in DataFrame: {col}")
            print(f"Available columns: {list(df_clean.columns)}")
            return

    df_clean = df_clean[df_clean[pk_column].notna().all(axis=1)].copy()
    if df_clean.empty:
        print("⚠️ DataFrame is empty after filtering null primary key values. No data to process.")
        return

    # กันซ้ำภายในชุดข้อมูลเอง (ตามคีย์ผสม)
    before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=pk_column, keep='last').copy()
    print(f"🔍 After removing duplicate composite keys in batch: {before} -> {len(df_clean)} rows")

    # ✅ เตรียมโหลด
    fresh_target_ops = create_fresh_target_engine()
    if not fresh_target_ops:
        print("❌ Failed to create fresh target engine. Exiting.")
        return

    table = Table(table_name, MetaData(), autoload_with=fresh_target_ops)

    # คอลัมน์ที่อนุญาตให้อัปเดต (ยกเว้น PK และ audit)
    update_cols = [
        c.name for c in table.columns
        if c.name not in pk_column + ['create_at', 'update_at']
    ]

    print("📤 Loading with conditional UPSERT (insert new, update only when changed)...")

    total = len(df_clean)
    done = 0
    batch_size = 5000

    try:
        for batch in chunker(df_clean, batch_size):
            # ทำความสะอาดให้ DB-friendly
            records = clean_records_for_db(batch.to_dict(orient='records'))

            # เติม create_at ตอน INSERT (ถ้าตารางไม่มี default)
            # หมายเหตุ: func.now() เป็น SQL expression ใช้ได้กับ executemany
            if 'create_at' in table.c:
                for r in records:
                    if 'create_at' not in r or r['create_at'] in (None, ''):
                        r['create_at'] = func.now()

            # สร้าง statement UPSERT แบบมี WHERE เฉพาะบรรทัดที่ “ต่างจริง”
            stmt = build_conditional_upsert(
                table=table,
                pk_cols=pk_column,
                update_allow_cols=update_cols,
                records=records
            )

            # ยิงเป็นทรานแซกชันเดียวของ batch
            max_retries = 3
            attempt = 0
            while attempt < max_retries:
                attempt += 1
                try:
                    with fresh_target_ops.begin() as conn:
                        conn.execute(text("SET statement_timeout = 300000"))
                        conn.execute(text("SET idle_in_transaction_session_timeout = 300000"))
                        conn.execute(stmt)
                    break
                except Exception as e:
                    print(f"    ❌ Error upserting batch (attempt {attempt}/{max_retries}): {e}")
                    if attempt >= max_retries:
                        print("    ⚠️ Max retries reached for this batch. Skipping.")
                    else:
                        time.sleep(2 ** attempt)

            done += len(batch)
            print(f"   ✅ upserted {done}/{total}")

        print("✅ Insert/Update completed (conditional upsert)")

    finally:
        try:
            fresh_target_ops.dispose()
            print("✅ Fresh target engine disposed successfully")
        except Exception as e:
            print(f"⚠️ Error disposing fresh target engine: {e}")
    
    # ✅ ปิด fresh target engine หลังจากเสร็จสิ้น
    try:
        if 'fresh_target_ops' in locals() and fresh_target_ops:
            fresh_target_ops.dispose()
            print("✅ Fresh target engine disposed successfully")
    except Exception as e:
        print(f"⚠️ Error disposing fresh target engine: {e}")
    
    # ✅ ตรวจสอบและปิด engines อื่นๆ ที่อาจยังไม่ได้ปิด
    engines_to_check = []
    
    # ตรวจสอบ engines ที่อาจมีอยู่ใน local scope
    for var_name in ['fresh_target', 'fresh_target_batch', 'fresh_target_update']:
        if var_name in locals():
            engine = locals()[var_name]
            if engine is not None:
                engines_to_check.append((var_name, engine))
    
    for engine_name, engine in engines_to_check:
        try:
            engine.dispose()
            print(f"✅ {engine_name} engine disposed successfully")
        except Exception as dispose_error:
            print(f"⚠️ {engine_name} engine disposal failed: {dispose_error}")
    
    # ✅ ตรวจสอบและปิด engines ที่อาจยังไม่ได้ปิดในส่วนท้าย
    try:
        # ตรวจสอบ engines ที่อาจมีอยู่ใน local scope
        for var_name in ['fresh_target', 'fresh_target_batch', 'fresh_target_update', 'fresh_target_ops']:
            if var_name in locals():
                engine = locals()[var_name]
                if engine is not None:
                    try:
                        engine.dispose()
                        print(f"✅ {var_name} engine disposed successfully")
                    except Exception as dispose_error:
                        print(f"⚠️ {var_name} engine disposal failed: {dispose_error}")
    except Exception as e:
        print(f"⚠️ Error during final engine cleanup: {e}")

@job
def fact_installment_payments_etl():
    load_installment_data(clean_installment_data(extract_installment_data()))
        
if __name__ == "__main__":
    df_raw = extract_installment_data()

    df_clean = clean_installment_data((df_raw))

    output_path = "fact_installment_payments.csv"
    df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"💾 Saved to {output_path}")

    # load_installment_data(df_clean)
    print("🎉 completed! Data upserted to fact_installment_payments.")