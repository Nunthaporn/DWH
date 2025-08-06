from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import create_engine, MetaData, Table, inspect, text
import datetime
import logging

# ✅ ตั้งค่า logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Load environment variables
load_dotenv()

# ✅ DB source (MariaDB) - เพิ่ม timeout และ connection pool
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300
    }
)
source_engine_task = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300
    }
)

# ✅ DB target (PostgreSQL) - เพิ่ม timeout และ connection pool
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={
        'connect_timeout': 60,
        'options': '-c statement_timeout=300000'  # 5 minutes timeout
    }
)

@op
def extract_sales_quotation_data():
    """Extract data from source databases"""
    try:
        logger.info("📦 เริ่มดึงข้อมูลจาก source databases...")
        
        # ✅ เพิ่มประสิทธิภาพ: ใช้ LIMIT และปรับปรุง query
        df_plan = pd.read_sql("""
            SELECT quo_num, type_insure, update_at, id_government_officer, status_gpf, quo_num_old,
                   status AS status_fssp
            FROM fin_system_select_plan 
            WHERE update_at BETWEEN '2025-01-01' AND '2025-08-06'
              AND type_insure IN ('ประกันรถ', 'ตรอ')
            ORDER BY update_at DESC
        """, source_engine)

        # ✅ ดึงเฉพาะข้อมูลที่จำเป็นจาก fin_order และเพิ่ม LIMIT
        df_order = pd.read_sql("""
            SELECT quo_num, order_number, chanel, datekey, status AS status_fo
            FROM fin_order
            WHERE quo_num IS NOT NULL
        """, source_engine_task)

        df_pay = pd.read_sql("""
            SELECT quo_num, update_at, numpay, show_price_ins, show_price_prb, show_price_total,
                   show_price_check, show_price_service, show_price_taxcar, show_price_fine,
                   show_price_addon, show_price_payment, distax, show_ems_price, show_discount_ins,
                   discount_mkt, discount_government, discount_government_fin,
                   discount_government_ins, coupon_addon, status AS status_fsp
            FROM fin_system_pay 
            WHERE update_at BETWEEN '2025-01-01' AND '2025-08-06'
              AND type_insure IN ('ประกันรถ', 'ตรอ')
            ORDER BY update_at DESC
        """, source_engine)

        logger.info(f"📦 df_plan shape: {df_plan.shape}")
        logger.info(f"📦 df_order shape: {df_order.shape}")
        logger.info(f"📦 df_pay shape: {df_pay.shape}")

        return df_plan, df_order, df_pay
        
    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการดึงข้อมูล: {e}")
        raise

@op
def clean_sales_quotation_data(inputs):
    """Clean and transform the extracted data"""
    try:
        df, df1, df2 = inputs
        
        logger.info("🧹 เริ่มทำความสะอาดข้อมูล...")
        
        # ✅ เพิ่มประสิทธิภาพ: ใช้ merge แบบเดียวและลดการ copy
        df_merged = df.merge(df1, on='quo_num', how='left')
        df_merged = df_merged.merge(df2, on='quo_num', how='left')
        
        # ✅ ทำความสะอาดข้อมูลแบบ vectorized
        df_merged = df_merged.replace(['nan', 'NaN', 'null', ''], np.nan)
        df_merged = df_merged.replace(r'^\s*$', np.nan, regex=True)
        df_merged = df_merged.where(pd.notnull(df_merged), None)

        # ✅ เปลี่ยนชื่อคอลัมน์
        column_mapping = {
            "quo_num": "quotation_num",
            "update_at_x": "quotation_date",
            "update_at_y": "transaction_date",
            "datekey": "order_time",
            "type_insure": "type_insurance",
            "id_government_officer": "rights_government",
            "status_gpf": "goverment_type",
            "quo_num_old": "quotation_num_old",
            "numpay": "installment_number",
            "show_price_ins": "ins_amount",
            "show_price_prb": "prb_amount",
            "show_price_total": "total_amount",
            "show_price_check": "show_price_check",
            "show_price_service": "service_price",
            "show_price_taxcar": "tax_car_price",
            "show_price_fine": "overdue_fine_price",
            "show_price_addon": "price_addon",
            "show_price_payment": "payment_amount",
            "distax": "tax_amount",
            "show_ems_price": "ems_amount",
            "show_discount_ins": "ins_discount",
            "discount_mkt": "mkt_discount",
            "discount_government": "goverment_discount",
            "discount_government_fin": "fin_goverment_discount",
            "discount_government_ins": "ins_goverment_discount",
            "coupon_addon": "discount_addon",
            "chanel": "contact_channel",
        }
        df_merged.rename(columns=column_mapping, inplace=True)

        # ✅ Add column: sale_team
        df_merged['sale_team'] = df_merged['type_insurance'].map({
            'ตรอ': 'ตรอ',
            'ประกันรถ': 'Motor agency'
        })
    
        # ✅ แปลงวันที่แบบ vectorized
        date_columns = ['transaction_date', 'order_time', 'quotation_date']
        for col in date_columns:
            if col in df_merged.columns:
                df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce').dt.strftime('%Y%m%d')
        
        # ✅ แทนที่ค่า installment_number แบบ vectorized
        if 'installment_number' in df_merged.columns:
            installment_mapping = {'0': '1', '03': '3', '06': '6', '08': '8'}
            df_merged['installment_number'] = df_merged['installment_number'].replace(installment_mapping)

        # ✅ สร้างคอลัมน์ status แบบ vectorized
        def create_status_mapping():
            """สร้าง mapping สำหรับ status"""
            return {
                ('wait', ''): '1',
                ('wait-key', ''): '1',
                ('sendpay', 'sendpay'): '2',
                ('sendpay', 'verify-wait'): '2',
                ('tran-succ', 'sendpay'): '2',
                ('tran-succ', 'verify-wait'): '2',
                ('cancel', '88'): 'cancel',
                ('delete', ''): 'delete',
                ('wait', 'sendpay'): '2',
                ('delete', 'sendpay'): 'delete',
                ('delete', 'wait'): 'delete',
                ('delete', 'wait-key'): 'delete',
                ('wait', 'wait'): '1',
                ('wait', 'wait-key'): '1',
                ('', 'wait'): '1',
                ('cancel', ''): 'cancel',
                ('cancel', 'cancel'): 'cancel',
                ('delete', 'delete'): 'delete',
                ('active', 'verify'): '6',
                ('active', 'success'): '8',
                ('active', ''): '8'
            }

        # ✅ ใช้ vectorized operations แทน apply
        status_mapping = create_status_mapping()
        
        # สร้าง key สำหรับ mapping
        df_merged['status_key'] = df_merged.apply(
            lambda row: (
                str(row.get('status_fssp') or '').strip(),
                str(row.get('status_fsp') or '').strip()
            ), axis=1
        )
        
        # แมปปิ้ง status
        df_merged['status'] = df_merged['status_key'].map(status_mapping)
        
        # กรณีพิเศษสำหรับ status_fo
        fo_mask = df_merged['status_fo'].notna()
        df_merged.loc[fo_mask, 'status'] = df_merged.loc[fo_mask, 'status_fo'].apply(
            lambda x: 'cancel' if x == '88' else x
        )
        
        # ลบคอลัมน์ที่ไม่จำเป็น
        df_merged.drop(columns=['status_fssp', 'status_fsp', 'status_fo', 'status_key'], inplace=True)

        # ✅ ลบข้อมูลซ้ำ
        df_merged.drop_duplicates(subset=['quotation_num'], keep='first', inplace=True)
        
        # ✅ แปลงคอลัมน์ตัวเลข
        numeric_columns = [
            'installment_number', 'show_price_check', 'price_product', 'ems_amount', 'service_price',
            'ins_amount', 'prb_amount', 'total_amount', 'tax_car_price', 'overdue_fine_price',
            'ins_discount', 'mkt_discount', 'payment_amount', 'price_addon', 'discount_addon',
            'goverment_discount', 'tax_amount', 'fin_goverment_discount', 'ins_goverment_discount'
        ]
        
        for col in numeric_columns:
            if col in df_merged.columns:
                df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')
                # แทนที่ inf และค่าที่เกินขอบเขต
                df_merged[col] = df_merged[col].replace([np.inf, -np.inf], None)

        # ✅ แปลงคอลัมน์ INT8
        int8_cols = [
            'transaction_date', 'order_time', 'installment_number', 'show_price_check',
            'price_product', 'ems_amount', 'service_price', 'quotation_date'
        ]
        
        for col in int8_cols:
            if col in df_merged.columns:
                df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')
                df_merged[col] = df_merged[col].astype('Int64')

        logger.info("✅ การทำความสะอาดข้อมูลเสร็จสิ้น")
        return df_merged
        
    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการทำความสะอาดข้อมูล: {e}")
        raise

@op
def load_sales_quotation_data(df: pd.DataFrame):
    # """Load data to target database with efficient upsert logic"""
    table_name = 'fact_sales_quotation'
    pk_column = 'quotation_num'
    
    # ✅ กรอง fact_sales_quotation ซ้ำจาก DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()
    
    # ✅ กรองข้อมูลที่ fact_sales_quotation ไม่เป็น None
    df = df[df[pk_column].notna()].copy()
    
    if df.empty:
        print("⚠️ No valid data to process")
        return

    # ✅ โหลดเฉพาะ fact_sales_quotation ที่มีอยู่ในข้อมูลใหม่ (ไม่โหลดทั้งหมด)
    # fact_sales_quotations = df[pk_column].tolist()
    
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(
            f"SELECT {pk_column} FROM {table_name}",
            conn
        )

    print(f"📊 New data: {len(df)} rows")
    print(f"📊 Existing data found: {len(df_existing)} rows")

    # ✅ กรอง fact_sales_quotation ซ้ำจากข้อมูลเก่า
    if not df_existing.empty:
        df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    # ✅ Identify fact_sales_quotation ใหม่ (ไม่มีใน DB)
    existing_ids = set(df_existing[pk_column]) if not df_existing.empty else set()
    new_ids = set(df[pk_column]) - existing_ids
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    # ✅ Identify fact_sales_quotation ที่มีอยู่แล้ว
    common_ids = set(df[pk_column]) & existing_ids
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    # ✅ ระบุคอลัมน์ที่ใช้เปรียบเทียบ (ยกเว้น key และ audit fields)
    exclude_columns = [pk_column, 'create_at', 'update_at']
    compare_cols = [
        col for col in df.columns
        if col not in exclude_columns
    ]
    
    print(f"🔍 Columns to compare for updates: {compare_cols}")
    print(f"🔍 Excluded columns (audit fields): {exclude_columns}")

    # ✅ เปรียบเทียบข้อมูลแบบ Vectorized (เร็วกว่า apply)
    df_to_update = pd.DataFrame()
    if not df_common_new.empty and not df_common_old.empty:
        # Merge ด้วย suffix (_new, _old)
        merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))
        
        # ตรวจสอบว่าคอลัมน์ที่มีอยู่ใน merged DataFrame
        available_cols = []
        for col in compare_cols:
            if f"{col}_new" in merged.columns and f"{col}_old" in merged.columns:
                available_cols.append(col)
        
        if available_cols:
            # สร้าง boolean mask สำหรับแถวที่มีความแตกต่าง
            diff_mask = pd.Series(False, index=merged.index)
            
            for col in available_cols:
                col_new = f"{col}_new"
                col_old = f"{col}_old"
                
                # เปรียบเทียบแบบ vectorized
                new_vals = merged[col_new]
                old_vals = merged[col_old]
                
                # จัดการ NaN values
                both_nan = (pd.isna(new_vals) & pd.isna(old_vals))
                different = (new_vals != old_vals) & ~both_nan
                
                diff_mask |= different
            
            # กรองแถวที่มีความแตกต่าง
            df_diff = merged[diff_mask].copy()
            
            if not df_diff.empty:
                # เตรียมข้อมูลสำหรับ update
                update_cols = [f"{col}_new" for col in available_cols]
                all_cols = [pk_column] + update_cols
                
                # ตรวจสอบว่าคอลัมน์ทั้งหมดมีอยู่ใน df_diff
                existing_cols = [col for col in all_cols if col in df_diff.columns]
                
                if len(existing_cols) > 1:
                    df_to_update = df_diff[existing_cols].copy()
                    # เปลี่ยนชื่อ column ให้ตรงกับตารางจริง
                    new_col_names = [pk_column] + [col.replace('_new', '') for col in existing_cols if col != pk_column]
                    df_to_update.columns = new_col_names

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_to_update)} rows")
    
    # ✅ แสดงตัวอย่างข้อมูลที่จะ insert และ update
    if not df_to_insert.empty:
        print("🔍 Sample data to INSERT:")
        sample_insert = df_to_insert.head(2)
        for col in ['sale_team', 'transaction_date', 'type_insurance']:
            if col in sample_insert.columns:
                print(f"   {col}: {sample_insert[col].tolist()}")
    
    if not df_to_update.empty:
        print("🔍 Sample data to UPDATE:")
        sample_update = df_to_update.head(2)
        for col in ['sale_team', 'transaction_date', 'type_insurance']:
            if col in sample_update.columns:
                print(f"   {col}: {sample_update[col].tolist()}")

    # ✅ Load table metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert (Batch operation)
    if not df_to_insert.empty:
        # แปลง DataFrame เป็น records
        records = []
        current_time = pd.Timestamp.now()
        for _, row in df_to_insert.iterrows():
            record = {}
            for col, value in row.items():
                if pd.isna(value) or value == pd.NaT or value == '':
                    record[col] = None
                else:
                    record[col] = value
            # ✅ ตั้งค่า audit fields สำหรับ insert
            record['create_at'] = current_time
            record['update_at'] = current_time
            records.append(record)
        
        # Insert แบบ batch
        with target_engine.begin() as conn:
            conn.execute(metadata.insert(), records)

    # ✅ Update (Batch operation)
    if not df_to_update.empty:
        # แปลง DataFrame เป็น records
        records = []
        for _, row in df_to_update.iterrows():
            record = {}
            for col, value in row.items():
                if pd.isna(value) or value == pd.NaT or value == '':
                    record[col] = None
                else:
                    record[col] = value
            records.append(record)
        
        # Update แบบ batch - เฉพาะคอลัมน์ที่ต้องการ update
        with target_engine.begin() as conn:
            for record in records:
                stmt = pg_insert(metadata).values(**record)
                # ✅ เฉพาะคอลัมน์ที่ต้องการ update (ไม่รวม audit fields)
                update_columns = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns
                    if c.name not in [pk_column, 'create_at', 'update_at']
                }
                # ✅ เพิ่ม update_at เป็นเวลาปัจจุบัน
                update_columns['update_at'] = pd.Timestamp.now()
                
                print(f"🔍 Updating columns for fact_sales_quotation {record.get(pk_column)}: {list(update_columns.keys())}")
                
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")

@job
def fact_sales_quotation_etl():
    # """Main ETL job for fact_sales_quotation"""
    data = extract_sales_quotation_data()
    df_clean = clean_sales_quotation_data(data)
    load_sales_quotation_data(df_clean)

if __name__ == "__main__":
    try:
        logger.info("🚀 เริ่มการประมวลผล fact_sales_quotation...")
        
        df_plan, df_order, df_pay = extract_sales_quotation_data()
        df_clean = clean_sales_quotation_data((df_plan, df_order, df_pay))

        # output_path = "fact_sales_quotation.xlsx"
        # df_clean.to_excel(output_path, index=False, engine='openpyxl')
        # print(f"💾 Saved to {output_path}")

        load_sales_quotation_data(df_clean)
        
        logger.info("🎉 completed! Data upserted to fact_sales_quotation.")
        
    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการประมวลผล: {e}")
        import traceback
        traceback.print_exc()
#         raise