from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
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
            SELECT quo_num, type_insure, datestart, id_government_officer, status_gpf, quo_num_old,
                   status AS status_fssp
            FROM fin_system_select_plan 
            WHERE datestart BETWEEN '2024-01-01' AND '2024-12-31'
              AND type_insure IN ('ประกันรถ', 'ตรอ')
            ORDER BY datestart DESC
        """, source_engine)

        # ✅ ดึงเฉพาะข้อมูลที่จำเป็นจาก fin_order และเพิ่ม LIMIT
        df_order = pd.read_sql("""
            SELECT quo_num, order_number, chanel, datekey, status AS status_fo
            FROM fin_order
            WHERE quo_num IS NOT NULL
        """, source_engine_task)

        df_pay = pd.read_sql("""
            SELECT quo_num, datestart, numpay, show_price_ins, show_price_prb, show_price_total,
                   show_price_check, show_price_service, show_price_taxcar, show_price_fine,
                   show_price_addon, show_price_payment, distax, show_ems_price, show_discount_ins,
                   discount_mkt, discount_government, discount_government_fin,
                   discount_government_ins, coupon_addon, status AS status_fsp
            FROM fin_system_pay 
            WHERE datestart BETWEEN '2024-01-01' AND '2024-12-31'
              AND type_insure IN ('ประกันรถ', 'ตรอ')
            ORDER BY datestart DESC
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
            "datestart_x": "quotation_date",
            "datestart_y": "transaction_date",
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
    """Load data to target database"""
    table_name = 'fact_sales_quotation'
    pk_column = 'quotation_num'
    
    try:
        # ✅ ตรวจสอบข้อมูล
        if df.empty:
            logger.warning("⚠️ ไม่มีข้อมูลที่จะประมวลผล")
            return
        
        if pk_column not in df.columns:
            logger.error(f"❌ ไม่พบคอลัมน์ {pk_column} ในข้อมูล")
            return
        
        logger.info(f"📊 เริ่มประมวลผลข้อมูล {len(df)} rows")

        # ลบข้อมูลซ้ำ
        df = df[~df[pk_column].duplicated(keep='first')].copy()
        logger.info(f"📊 ข้อมูลหลังจากลบซ้ำ: {len(df)} rows")

        # ✅ ดึง quotation_num ที่ valid
        quotation_nums = df[pk_column].dropna().unique()
        if len(quotation_nums) == 0:
            logger.warning("⚠️ ไม่มี quotation_num ที่ valid")
            return
        
        logger.info(f"📊 พบ quotation_num ที่ valid: {len(quotation_nums)} รายการ")

        # ✅ ตรวจสอบข้อมูลที่มีอยู่แบบ batch
        existing_ids = pd.DataFrame(columns=[pk_column])
        if len(quotation_nums) > 0:
            try:
                batch_size = 1000
                existing_ids_list = []
                
                for i in range(0, len(quotation_nums), batch_size):
                    batch = quotation_nums[i:i + batch_size]
                    if len(batch) == 0:
                        continue
                        
                    placeholders = ','.join(['%s'] * len(batch))
                    query = f"SELECT {pk_column} FROM {table_name} WHERE {pk_column} IN ({placeholders})"
                    params = [str(qnum) for qnum in batch]
                    
                    try:
                        with target_engine.connect() as conn:
                            batch_existing = pd.read_sql(query, conn, params=params)
                            if not batch_existing.empty:
                                existing_ids_list.append(batch_existing)
                    except Exception as batch_error:
                        logger.warning(f"⚠️ เกิดข้อผิดพลาดใน batch {i//batch_size + 1}: {batch_error}")
                        continue
                
                existing_ids = pd.concat(existing_ids_list, ignore_index=True) if existing_ids_list else pd.DataFrame(columns=[pk_column])
                logger.info(f"✅ พบข้อมูลที่มีอยู่ {len(existing_ids)} rows")
            except Exception as e:
                logger.warning(f"⚠️ เกิดข้อผิดพลาดในการตรวจสอบข้อมูล: {e}")
                existing_ids = pd.DataFrame(columns=[pk_column])
        
        # แยกข้อมูลสำหรับ insert และ update
        if existing_ids.empty or pk_column not in existing_ids.columns:
            existing_quotation_nums = set()
        else:
            existing_quotation_nums = set(existing_ids[pk_column].astype(str))
        
        new_quotation_nums = set(quotation_nums.astype(str)) - existing_quotation_nums
        common_quotation_nums = set(quotation_nums.astype(str)) & existing_quotation_nums

        df_to_insert = df[df[pk_column].astype(str).isin(new_quotation_nums)].copy()
        df_to_update = df[df[pk_column].astype(str).isin(common_quotation_nums)].copy()

        logger.info(f"🆕 Insert: {len(df_to_insert)} rows")
        logger.info(f"🔄 Update: {len(df_to_update)} rows")

        # ✅ Load metadata
        metadata = Table(table_name, MetaData(), autoload_with=target_engine)

        # ✅ Batch Insert new rows
        if not df_to_insert.empty:
            df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
            dropped = len(df_to_insert) - len(df_to_insert_valid)
            if dropped > 0:
                logger.warning(f"⚠️ Skipped {dropped} rows with null {pk_column}")

            # ทำความสะอาดข้อมูลก่อน insert
            df_to_insert_valid = df_to_insert_valid.where(pd.notnull(df_to_insert_valid), None)
            df_to_insert_valid = df_to_insert_valid.replace([np.inf, -np.inf], None)

            if not df_to_insert_valid.empty:
                try:
                    logger.info(f"💾 เริ่ม insert ข้อมูล {len(df_to_insert_valid)} rows...")
                    
                    df_to_insert_valid.to_sql(
                        table_name, 
                        target_engine, 
                        if_exists='append', 
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    
                    logger.info(f"✅ Insert สำเร็จ {len(df_to_insert_valid)} rows")
                except Exception as e:
                    logger.error(f"❌ เกิดข้อผิดพลาดในการ insert: {e}")
                    # Fallback to individual inserts
                    logger.info("🔄 ลองใช้วิธี insert แบบ individual...")
                    with target_engine.begin() as conn:
                        records = df_to_insert_valid.to_dict(orient='records')
                        for i, record in enumerate(records):
                            if i % 1000 == 0:
                                logger.info(f"📊 Inserted {i}/{len(records)} rows...")
                            stmt = pg_insert(metadata).values(**record)
                            stmt = stmt.on_conflict_do_nothing(index_elements=[pk_column])
                            conn.execute(stmt)
                    logger.info(f"✅ Insert สำเร็จ {len(records)} rows (fallback method)")

        # ✅ Batch Update existing rows
        if not df_to_update.empty:
            # ดึงข้อมูลเดิมสำหรับเปรียบเทียบ
            df_existing_for_update = pd.DataFrame()
            if len(common_quotation_nums) > 0:
                try:
                    batch_size = 1000
                    df_existing_list = []
                    
                    for i in range(0, len(common_quotation_nums), batch_size):
                        batch = list(common_quotation_nums)[i:i + batch_size]
                        if len(batch) == 0:
                            continue
                            
                        update_placeholders = ','.join(['%s'] * len(batch))
                        update_query = f"SELECT * FROM {table_name} WHERE {pk_column} IN ({update_placeholders})"
                        update_params = [str(qnum) for qnum in batch]
                        
                        try:
                            with target_engine.connect() as conn:
                                batch_existing = pd.read_sql(update_query, conn, params=update_params)
                                if not batch_existing.empty:
                                    df_existing_list.append(batch_existing)
                        except Exception as batch_error:
                            logger.warning(f"⚠️ เกิดข้อผิดพลาดใน update batch {i//batch_size + 1}: {batch_error}")
                            continue
                    
                    df_existing_for_update = pd.concat(df_existing_list, ignore_index=True) if df_existing_list else pd.DataFrame()
                    logger.info(f"✅ ดึงข้อมูลเดิมสำเร็จ {len(df_existing_for_update)} rows")
                except Exception as e:
                    logger.warning(f"⚠️ เกิดข้อผิดพลาดในการดึงข้อมูลเดิม: {e}")
                    df_existing_for_update = pd.DataFrame()
            
            # เปรียบเทียบข้อมูลแบบ vectorized
            if not df_existing_for_update.empty:
                exclude_columns = [pk_column, 'agent_id', 'customer_id', 'car_id', 'sales_id',
                                   'order_type_id', 'payment_plan_id', 'create_at', 'update_at']
                
                compare_cols = [col for col in df.columns if col not in exclude_columns]
                
                # ใช้ merge แทน iterrows
                merged = df_to_update.merge(df_existing_for_update, on=pk_column, suffixes=('_new', '_old'))
                
                # เปรียบเทียบแบบ vectorized
                changed_mask = pd.Series([False] * len(merged), index=merged.index)
                
                for col in compare_cols:
                    col_new = f"{col}_new"
                    col_old = f"{col}_old"
                    
                    if col_new in merged.columns and col_old in merged.columns:
                        mask = (merged[col_new] != merged[col_old]) | (merged[col_new].isna() != merged[col_old].isna())
                        changed_mask = changed_mask | mask
                
                # ดึงแถวที่มีการเปลี่ยนแปลง
                changed_rows_df = merged[changed_mask][[pk_column] + compare_cols].copy()
                
                if not changed_rows_df.empty:
                    logger.info(f"🔄 Updating {len(changed_rows_df)} changed rows...")
                    
                    # ทำความสะอาดข้อมูล
                    changed_rows_df = changed_rows_df.where(pd.notnull(changed_rows_df), None)
                    changed_rows_df = changed_rows_df.replace([np.inf, -np.inf], None)
                    
                    # ใช้ to_sql สำหรับ batch update
                    try:
                        # สร้าง temporary table สำหรับ update
                        temp_table_name = f"temp_update_{table_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        
                        # Insert ข้อมูลใหม่ลง temporary table
                        changed_rows_df.to_sql(
                            temp_table_name,
                            target_engine,
                            if_exists='replace',
                            index=False,
                            method='multi',
                            chunksize=1000
                        )
                        
                        # ใช้ SQL UPDATE แบบ batch
                        update_columns = ', '.join([f"{col} = t.{col}" for col in compare_cols])
                        update_sql = f"""
                            UPDATE {table_name} 
                            SET {update_columns}, update_at = NOW()
                            FROM {temp_table_name} t
                            WHERE {table_name}.{pk_column} = t.{pk_column}
                        """
                        
                        with target_engine.begin() as conn:
                            conn.execute(update_sql)
                        
                        # ลบ temporary table
                        with target_engine.begin() as conn:
                            conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                        
                        logger.info(f"✅ Update สำเร็จ {len(changed_rows_df)} rows")
                    except Exception as e:
                        logger.error(f"❌ เกิดข้อผิดพลาดในการ update: {e}")
                        # Fallback to individual updates
                        logger.info("🔄 ลองใช้วิธี update แบบ individual...")
                        with target_engine.begin() as conn:
                            for i, (_, row) in enumerate(changed_rows_df.iterrows()):
                                if i % 1000 == 0:
                                    logger.info(f"📊 Updated {i}/{len(changed_rows_df)} rows...")
                                record = row.to_dict()
                                stmt = pg_insert(metadata).values(**record)
                                update_dict = {
                                    c.name: stmt.excluded[c.name]
                                    for c in metadata.columns if c.name not in [pk_column, 'create_at', 'update_at']
                                }
                                update_dict['update_at'] = datetime.datetime.now()
                                stmt = stmt.on_conflict_do_update(
                                    index_elements=[pk_column],
                                    set_=update_dict
                                )
                                conn.execute(stmt)
                        logger.info(f"✅ Update สำเร็จ {len(changed_rows_df)} rows (fallback method)")
                else:
                    logger.info("✅ No changes detected for update.")
            else:
                logger.info("✅ No existing data to update.")

        logger.info("🎉 Insert/update completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการ load ข้อมูล: {e}")
        raise

@job
def fact_sales_quotation_etl():
    """Main ETL job for fact_sales_quotation"""
    load_sales_quotation_data(clean_sales_quotation_data(extract_sales_quotation_data()))

if __name__ == "__main__":
    try:
        logger.info("🚀 เริ่มการประมวลผล fact_sales_quotation...")
        
        df_plan, df_order, df_pay = extract_sales_quotation_data()
        df_clean = clean_sales_quotation_data((df_plan, df_order, df_pay))
        load_sales_quotation_data(df_clean)
        
        logger.info("🎉 completed! Data upserted to fact_sales_quotation.")
        
    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการประมวลผล: {e}")
        import traceback
        traceback.print_exc()
        raise