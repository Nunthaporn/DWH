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
        
        df_plan = pd.read_sql("""
            SELECT quo_num, type_insure, update_at, id_government_officer, status_gpf, quo_num_old,
                   status AS status_fssp, type_car, chanel_key, id_cus  
            FROM fin_system_select_plan 
            WHERE update_at BETWEEN '2025-01-01' AND '2025-08-31'
                AND id_cus NOT LIKE '%%FIN-TestApp%%'
                AND id_cus NOT LIKE '%%FIN-TestApp3%%'
                AND id_cus NOT LIKE '%%FIN-TestApp2%%'
                AND id_cus NOT LIKE '%%FIN-TestApp-2025%%'
                AND id_cus NOT LIKE '%%FIN-Tester1%%'
                AND id_cus NOT LIKE '%%FIN-Tester2%%'
        """, source_engine)

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
        """, source_engine)

        df_risk = pd.read_sql("""
            SELECT quo_num, type 
            FROM fin_detail_plan_risk  
            WHERE type = 'คอนโด'
        """, source_engine)

        df_pa = pd.read_sql("""
            SELECT quo_num, special_package 
            FROM fin_detail_plan_pa  
            WHERE special_package = 'CHILD'
        """, source_engine)

        df_health = pd.read_sql("""
            SELECT quo_num, special_package 
            FROM fin_detail_plan_health  
            WHERE special_package = 'CHILD'
        """, source_engine)

        df_wp = pd.read_sql("""
            SELECT cuscode as id_cus, display_permission
            FROM wp_users 
            WHERE display_permission IN ('สำนักงานฟิน', 'หน้าร้านฟิน')
                AND cuscode NOT LIKE '%%FIN-TestApp%%'
                AND cuscode NOT LIKE '%%FIN-TestApp3%%'
                AND cuscode NOT LIKE '%%FIN-TestApp2%%'
                AND cuscode NOT LIKE '%%FIN-TestApp-2025%%'
                AND cuscode NOT LIKE '%%FIN-TestApp%%'
                AND cuscode NOT LIKE '%%FIN-Tester1%%'
                AND cuscode NOT LIKE '%%FIN-Tester2%%';
        """, source_engine)

        logger.info(f"📦 Shapes: plan={df_plan.shape}, order={df_order.shape}, pay={df_pay.shape}, risk={df_risk.shape}, pa={df_pa.shape}, health={df_health.shape}, wp={df_wp.shape}")
        return df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการดึงข้อมูล: {e}")
        raise

@op
def clean_sales_quotation_data(inputs):
    try:
        df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp = inputs
        logger.info("🧹 เริ่มทำความสะอาดข้อมูล...")

        # ✅ กรองข้อมูลซ้ำใน df_pay
        df_pay['update_at'] = pd.to_datetime(df_pay['update_at'], errors='coerce')
        df_pay = df_pay.sort_values('update_at').drop_duplicates(subset='quo_num', keep='last')

        # ✅ Merge ข้อมูลทั้งหมด
        df_merged = df_plan.merge(df_order, on='quo_num', how='left')
        df_merged = df_merged.merge(df_pay, on='quo_num', how='left', suffixes=('', '_pay'))
        df_merged = df_merged.merge(df_risk, on='quo_num', how='left', suffixes=('', '_risk'))
        df_merged = df_merged.merge(df_pa, on='quo_num', how='left', suffixes=('', '_pa'))
        df_merged = df_merged.merge(df_health, on='quo_num', how='left', suffixes=('', '_health'))
        df_merged = df_merged.merge(df_wp, on='id_cus', how='left')

        logger.info(f"📊 Shape after merge: {df_merged.shape}")

        # ✅ แปลงค่า null ให้เป็น None
        df_merged.replace(['nan', 'NaN', 'null', '', 'NULL'], np.nan, inplace=True)
        df_merged.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        df_merged = df_merged.where(pd.notnull(df_merged), None)

        # ✅ Rename columns อย่างชัดเจน
        col_map = {
            'quo_num': 'quotation_num',
            'update_at': 'quotation_date',
            'update_at_pay': 'transaction_date',
            'datekey': 'order_time',
            'type_insure': 'type_insurance',
            'id_government_officer': 'rights_government',
            'status_gpf': 'goverment_type',
            'quo_num_old': 'quotation_num_old',
            'numpay': 'installment_number',
            'show_price_ins': 'ins_amount',
            'show_price_prb': 'prb_amount',
            'show_price_total': 'total_amount',
            'show_price_check': 'show_price_check',
            'show_price_service': 'service_price',
            'show_price_taxcar': 'tax_car_price',
            'show_price_fine': 'overdue_fine_price',
            'show_price_addon': 'price_addon',
            'show_price_payment': 'payment_amount',
            'distax': 'tax_amount',
            'show_ems_price': 'ems_amount',
            'show_discount_ins': 'ins_discount',
            'discount_mkt': 'mkt_discount',
            'discount_government': 'goverment_discount',
            'discount_government_fin': 'fin_goverment_discount',
            'discount_government_ins': 'ins_goverment_discount',
            'coupon_addon': 'discount_addon',
            'chanel': 'contact_channel'
        }
        df_merged.rename(columns=col_map, inplace=True)

        # ✅ แปลงวันที่แบบไม่เติม default
        for col in ['quotation_date', 'transaction_date', 'order_time']:
            if col in df_merged.columns:
                df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce').dt.strftime('%Y%m%d')
                df_merged[col] = df_merged[col].where(pd.notnull(df_merged[col]), None)

        # ✅ ลบแถวที่ไม่มี transaction_date
        if 'transaction_date' in df_merged.columns:
            before_drop = len(df_merged)
            df_merged = df_merged[df_merged['transaction_date'].notna()].copy()
            after_drop = len(df_merged)
            print(f"🧹 ลบแถวที่ไม่มี transaction_date: {before_drop - after_drop} แถวถูกลบ")

        # ✅ สร้าง sale_team
        def assign_sale_team(row):
            id_cus = str(row.get('id_cus') or '')
            type_insurance = str(row.get('type_insurance') or '').strip().lower()
            type_car = str(row.get('type_car') or '').strip().lower()
            chanel_key = str(row.get('chanel_key') or '').strip()
            special_package = str(row.get('special_package') or '').strip().upper()
            special_package_health = str(row.get('special_package_health') or '').strip().upper()

            if id_cus.startswith('FTR'):
                return 'Telesales'
            if type_car == 'fleet':
                return 'fleet'
            if type_car == 'ตะกาฟุล':
                return 'ตะกาฟุล'
            if type_insurance == 'ประกันรถ':
                return 'Motor agency'
            if type_insurance == 'ตรอ':
                return 'ตรอ'
            if chanel_key == 'CHILD':
                return 'ประกันเด็ก'
            if chanel_key in ['หน้าร้าน', 'หน้าร้านฟิน', 'สำนักงานฟิน']:
                return 'หน้าร้าน'
            if chanel_key == 'WEB-SUBBROKER':
                return 'Subbroker'
            if str(row.get('type') or '').strip() == 'คอนโด':
                return 'ประกันคอนโด'
            if special_package == 'CHILD' or special_package_health == 'CHILD':
                return 'ประกันเด็ก'
            if row.get('display_permission') in ['หน้าร้านฟิน', 'สำนักงานฟิน']:
                return 'หน้าร้าน'
            if not type_insurance and not type_car:
                return 'N/A'
            return 'Non Motor'

        df_merged['sale_team'] = df_merged.apply(assign_sale_team, axis=1)

        # ✅ ลบคอลัมน์ที่ไม่จำเป็น
        df_merged.drop(columns=[
            'id_cus', 'type_car', 'chanel_key', 'special_package',
            'special_package_health', 'type', 'display_permission'
        ], errors='ignore', inplace=True)

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
                ('active', 'verify'): '3',
                ('active', 'success'): '8',
                ('active', ''): '8',
                ('active', 'success-waitinstall'): '8',
                ('active', 'sendpay'): '2',
                ('delete', 'verify'): 'delete',
                ('wait', 'verify'): '2',
                ('active', 'cancel'): 'cancel',
                ('wait-pay', ''): '1',
                ('', 'verify'): '2',
                ('tran-succ', 'wait-key'): '2',
                ('', 'cancel'): 'cancel',
                ('delelte', 'sendpay'): 'delete',  # แก้ typo 'delelte'
                ('cancel', 'success'): 'cancel',
                ('sendpay', ''): '2',
                ('wait-cancel', 'wait'): 'cancel',
                ('cancel', 'sendpay'): 'cancel',
                ('cancel', 'wait'): 'cancel',
                ('active', 'wait'): '1',
                ('tran-succ', 'verify'): '2',
                ('active', 'verify-wait'): '1',
                ('cancel', 'verify'): 'cancel',
                ('wait', 'cancel'): 'cancel',
                ('tran-succ', 'cancel'): 'cancel',
                ('', 'success'): '8',
                ('tran-succ', 'wait-confirm'): '2',
                ('wait-key', 'sendpay'): '2',
                ('wait-key', 'wait-key'): '1',
                ('wait-pay', 'sendpay'): '2',
            }

        status_mapping = create_status_mapping()

        # ✅ key สำหรับ mapping
        df_merged['status_key'] = df_merged.apply(
            lambda row: (
                str(row.get('status_fssp') or '').strip(),
                str(row.get('status_fsp') or '').strip()
            ), axis=1
        )

        # ✅ แมปปิ้ง status เบื้องต้น
        df_merged['status'] = df_merged['status_key'].map(status_mapping)

        # ✅ override ด้วย status_fo (เดิม)
        fo_mask = df_merged['status_fo'].notna()
        df_merged.loc[fo_mask, 'status'] = df_merged.loc[fo_mask, 'status_fo'].apply(
            lambda x: 'cancel' if x == '88' else x
        )

        # ✅ กฎทับสุดท้าย (Priority สูงสุด): ถ้า status_key มี 'delete' หรือ 'cancel' ให้ทับทันที
        #    เช่น ('delete','success') -> delete, ('cancel','success') -> cancel
        df_merged['has_delete'] = df_merged['status_key'].apply(lambda t: isinstance(t, tuple) and ('delete' in t))
        df_merged['has_cancel'] = df_merged['status_key'].apply(lambda t: isinstance(t, tuple) and ('cancel' in t))

        # delete ชนะทุกกรณี
        df_merged.loc[df_merged['has_delete'] == True, 'status'] = 'delete'
        # ถ้าไม่มี delete แต่มี cancel ให้เป็น cancel
        df_merged.loc[(df_merged['has_delete'] != True) & (df_merged['has_cancel'] == True), 'status'] = 'cancel'

        # เก็บบ้านให้เรียบร้อย
        df_merged.drop(columns=['has_delete', 'has_cancel'], inplace=True, errors='ignore')

        # ✅ แปลงค่า numeric
        numeric_cols = [
            'installment_number', 'show_price_check', 'price_product', 'ems_amount', 'service_price',
            'ins_amount', 'prb_amount', 'total_amount', 'tax_car_price', 'overdue_fine_price',
            'ins_discount', 'mkt_discount', 'payment_amount', 'price_addon', 'discount_addon',
            'goverment_discount', 'tax_amount', 'fin_goverment_discount', 'ins_goverment_discount'
        ]
        for col in numeric_cols:
            if col in df_merged.columns:
                df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce').replace([np.inf, -np.inf], None)

        logger.info("✅ การทำความสะอาดข้อมูลเสร็จสิ้น")
        return df_merged

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการทำความสะอาดข้อมูล: {e}")
        raise

@op
def load_sales_quotation_data(df: pd.DataFrame):
    table_name = 'fact_sales_quotation'
    pk_column = 'quotation_num'
    
    # ✅ กรองซ้ำใน DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()
    # ✅ กรองเฉพาะที่ key ไม่เป็น None
    df = df[df[pk_column].notna()].copy()
    
    if df.empty:
        print("⚠️ No valid data to process")
        return
    
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT {pk_column} FROM {table_name}", conn)

    print(f"📊 New data: {len(df)} rows")
    print(f"📊 Existing data found: {len(df_existing)} rows")

    if not df_existing.empty:
        df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    existing_ids = set(df_existing[pk_column]) if not df_existing.empty else set()
    new_ids = set(df[pk_column]) - existing_ids
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    common_ids = set(df[pk_column]) & existing_ids
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    exclude_columns = [pk_column, 'create_at', 'update_at']
    compare_cols = [col for col in df.columns if col not in exclude_columns]
    
    print(f"🔍 Columns to compare for updates: {compare_cols}")
    print(f"🔍 Excluded columns (audit fields): {exclude_columns}")

    df_to_update = pd.DataFrame()
    if not df_common_new.empty and not df_common_old.empty:
        merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))
        available_cols = [col for col in compare_cols if f"{col}_new" in merged.columns and f"{col}_old" in merged.columns]

        if available_cols:
            diff_mask = pd.Series(False, index=merged.index)
            for col in available_cols:
                new_vals = merged[f"{col}_new"]
                old_vals = merged[f"{col}_old"]
                both_nan = (pd.isna(new_vals) & pd.isna(old_vals))
                different = (new_vals != old_vals) & ~both_nan
                diff_mask |= different

            df_diff = merged[diff_mask].copy()
            if not df_diff.empty:
                update_cols = [f"{col}_new" for col in available_cols]
                all_cols = [pk_column] + update_cols
                existing_cols = [c for c in all_cols if c in df_diff.columns]
                if len(existing_cols) > 1:
                    df_to_update = df_diff[existing_cols].copy()
                    new_col_names = [pk_column] + [c.replace('_new', '') for c in existing_cols if c != pk_column]
                    df_to_update.columns = new_col_names

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_to_update)} rows")
    
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

    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert (Batch)
    if not df_to_insert.empty:
        records = []
        current_time = pd.Timestamp.now()
        for _, row in df_to_insert.iterrows():
            record = {}
            for col, value in row.items():
                if pd.isna(value) or value == pd.NaT or value == '':
                    record[col] = None
                else:
                    record[col] = value
            record['create_at'] = current_time
            record['update_at'] = current_time
            records.append(record)
        with target_engine.begin() as conn:
            conn.execute(metadata.insert(), records)

    # ✅ Update (Batch upsert)
    if not df_to_update.empty:
        records = []
        for _, row in df_to_update.iterrows():
            record = {}
            for col, value in row.items():
                if pd.isna(value) or value == pd.NaT or value == '':
                    record[col] = None
                else:
                    record[col] = value
            records.append(record)

        with target_engine.begin() as conn:
            for record in records:
                stmt = pg_insert(metadata).values(**record)
                update_columns = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns
                    if c.name not in [pk_column, 'create_at', 'update_at']
                }
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
    data = extract_sales_quotation_data()
    df_clean = clean_sales_quotation_data(data)
    load_sales_quotation_data(df_clean)

if __name__ == "__main__":
    try:
        logger.info("🚀 เริ่มการประมวลผล fact_sales_quotation...")
        
        df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp = extract_sales_quotation_data()
        df_clean = clean_sales_quotation_data((df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp))

        output_path = "fact_sales_quotation.xlsx"
        df_clean.to_excel(output_path, index=False, engine='openpyxl')
        print(f"💾 Saved to {output_path}")

        # logger.info("🎉 completed! Data upserted to fact_sales_quotation.")
        
    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการประมวลผล: {e}")
        import traceback
        traceback.print_exc()
        raise
