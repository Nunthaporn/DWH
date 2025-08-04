from dagster import op, job
import pandas as pd
import numpy as np
import os
import json
import re
import gc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime

# ตั้งค่า logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# DB: Source (MariaDB)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600
)

# DB: Target (PostgreSQL)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600
)

@op
def extract_fact_check_price():
    """Extract data with better memory management"""
    try:
        logger.info("📊 Starting data extraction...")
        
        # Process logs data in smaller chunks
        logger.info("📊 Extracting logs data...")
        chunks_logs = []
        chunk_count = 0
        
        # ใช้ query ที่มี LIMIT เพื่อป้องกัน memory overflow
        query_logs = """
            SELECT cuscode, brand, series, subseries, year, no_car, type, repair_type,
                   assured_insurance_capital1, camera, addon, quo_num, create_at, results, selected, carprovince
            FROM fin_customer_logs_B2B
            WHERE create_at BETWEEN '2025-01-01' AND '2025-08-04'
            ORDER BY create_at
        """
        
        for chunk in pd.read_sql(query_logs, source_engine, chunksize=1000):  # ลด chunksize ลง
            chunks_logs.append(chunk)
            chunk_count += 1
            if chunk_count % 5 == 0:  # ลดความถี่ของ logging
                logger.info(f"   Processed {chunk_count} chunks...")
                gc.collect()  # Force garbage collection
        
        logger.info(f"✅ Concatenating {len(chunks_logs)} chunks...")
        df_logs = pd.concat(chunks_logs, ignore_index=True)
        del chunks_logs  # Free memory
        gc.collect()
        logger.info(f"✅ Logs shape: {df_logs.shape}")

        # Process checkprice data in smaller chunks
        logger.info("📊 Extracting checkprice data...")
        chunks_check = []
        chunk_count = 0
        
        query_check = """
            SELECT id_cus, datekey, brand, model, submodel, yearcar, idcar, nocar, type_ins,
                   company, tunprakan, deduct, status, type_driver, type_camera, type_addon, status_send
            FROM fin_checkprice
            ORDER BY datekey
        """
        
        for chunk in pd.read_sql(query_check, source_engine, chunksize=1000):  # ลด chunksize ลง
            chunks_check.append(chunk)
            chunk_count += 1
            if chunk_count % 5 == 0:  # ลดความถี่ของ logging
                logger.info(f"   Processed {chunk_count} chunks...")
                gc.collect()
        
        logger.info(f"✅ Concatenating {len(chunks_check)} chunks...")
        df_checkprice = pd.concat(chunks_check, ignore_index=True)
        del chunks_check  # Free memory
        gc.collect()
        logger.info(f"✅ Checkprice shape: {df_checkprice.shape}")

        return df_logs, df_checkprice
    
    except SQLAlchemyError as e:
        logger.error(f"❌ Database error in extract_fact_check_price: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Error in extract_fact_check_price: {str(e)}")
        raise

@op
def clean_fact_check_price(data):
    """Clean data with better memory management"""
    try:
        df_logs, df_checkprice = data
        logger.info(f"🧹 Cleaning data - Logs: {df_logs.shape}, Checkprice: {df_checkprice.shape}")

        province_list = ["กรุงเทพมหานคร", "กระบี่", "กาญจนบุรี", "กาฬสินธุ์", "กำแพงเพชร",
        "ขอนแก่น", "จันทบุรี", "ฉะเชิงเทรา", "ชลบุรี", "ชัยนาท", "ชัยภูมิ",
        "ชุมพร", "เชียงใหม่", "เชียงราย", "ตรัง", "ตราด", "ตาก", "นครนายก",
        "นครปฐม", "นครพนม", "นครราชสีมา", "นครศรีธรรมราช", "นครสวรรค์",
        "นนทบุรี", "นราธิวาส", "น่าน", "บึงกาฬ", "บุรีรัมย์", "ปทุมธานี",
        "ประจวบคีรีขันธ์", "ปราจีนบุรี", "ปัตตานี", "พระนครศรีอยุธยา",
        "พังงา", "พัทลุง", "พิจิตร", "พิษณุโลก", "เพชรบุรี", "เพชรบูรณ์",
        "แพร่", "พะเยา", "ภูเก็ต", "มหาสารคาม", "มุกดาหาร", "แม่ฮ่องสอน",
        "ยะลา", "ยโสธร", "ระนอง", "ระยอง", "ราชบุรี", "ร้อยเอ็ด", "ลพบุรี",
        "ลำปาง", "ลำพูน", "เลย", "ศรีสะเกษ", "สกลนคร", "สงขลา", "สตูล",
        "สมุทรปราการ", "สมุทรสงคราม", "สมุทรสาคร", "สระแก้ว", "สระบุรี",
        "สิงห์บุรี", "สุโขทัย", "สุพรรณบุรี", "สุราษฎร์ธานี", "สุรินทร์",
        "หนองคาย", "หนองบัวลำภู", "อ่างทอง", "อุดรธานี", "อุทัยธานี",
        "อุตรดิตถ์", "อุบลราชธานี", "อำนาจเจริญ"]  

        def extract_company_names(x):
            if pd.isnull(x): return [None]*4
            try:
                data = json.loads(x)
                names = [d.get('company_name') for d in data if isinstance(d, dict)] if isinstance(data, list) else [data.get('company_name')]
                return (names + [None]*4)[:4]
            except:
                return [None]*4

        def extract_selected(x):
            if pd.isnull(x): return None
            try:
                data = json.loads(x)
                if isinstance(data, list) and data: return data[0].get('company_name')
                if isinstance(data, dict): return data.get('company_name')
                return None
            except:
                return None

        logger.info("🧹 Processing logs data...")
        logs = df_logs.copy()
        
        # แยกการประมวลผลเพื่อลด memory usage
        logger.info("   Extracting company names...")
        company_names = logs['results'].apply(extract_company_names).apply(pd.Series)
        logs[['company_name1', 'company_name2', 'company_name3', 'company_name4']] = company_names
        del company_names
        gc.collect()
        
        logger.info("   Extracting selected companies...")
        logs['selecteds'] = logs['selected'].apply(extract_selected)

        logs['camera'] = logs['camera'].map({'yes': 'มีกล้องหน้ารถ', 'no': 'ไม่มีกล้องหน้ารถ'})
        logs['addon'] = logs['addon'].map({'ไม่มี': 'ไม่มีการต่อเติม'})

        logs.rename(columns={
            'cuscode': 'id_cus', 'brand': 'brand', 'series': 'model', 'subseries': 'submodel',
            'year': 'yearcar', 'no_car': 'car_code', 'assured_insurance_capital1': 'sum_insured',
            'camera': 'type_camera', 'addon': 'type_addon', 'quo_num': 'select_quotation',
            'create_at': 'transaction_date', 'carprovince': 'province_car'
        }, inplace=True)

        logs['type_insurance'] = 'ชั้น' + logs['type'].astype(str) + logs['repair_type'].astype(str)
        logs['input_type'] = 'auto'
        logs.drop(columns=['type', 'repair_type', 'results', 'selected'], inplace=True)

        def split_company(x):
            if pd.isnull(x): return [None]*4
            parts = [i.strip() for i in x.split('/')]
            return (parts + [None]*4)[:4]

        def extract_plate(x):
            if pd.isnull(x): return None
            try:
                x = str(x)
                for p in province_list:
                    x = x.replace(p, '')
                matches = re.findall(r'\d{1,2}[ก-ฮ]{1,2}\d{1,4}', x.replace('-', '').replace('/', ''))
                return matches[0] if matches else None
            except:
                return None

        logger.info("🧹 Processing checkprice data...")
        check = df_checkprice.copy()
        
        # แยกการประมวลผลเพื่อลด memory usage
        logger.info("   Splitting company names...")
        company_names = check['company'].apply(split_company).apply(pd.Series)
        check[['company_name1', 'company_name2', 'company_name3', 'company_name4']] = company_names
        del company_names
        gc.collect()
        
        logger.info("   Extracting car plates...")
        check['id_car'] = check['idcar'].apply(extract_plate)
        check['province_car'] = check['idcar'].apply(lambda x: next((p for p in province_list if p in str(x)), None))
        check.drop(columns=['company', 'idcar'], inplace=True)

        check.rename(columns={
            'datekey': 'transaction_date', 'nocar': 'car_code', 'type_ins': 'type_insurance',
            'tunprakan': 'sum_insured', 'deduct': 'deductible'
        }, inplace=True)
        check['input_type'] = 'manual'

        logger.info("🧹 Combining datasets...")
        df_combined = pd.concat([logs, check], ignore_index=True, sort=False)
        del logs, check  # Free memory
        gc.collect()

        df_combined = df_combined.replace(r'^\s*$', np.nan, regex=True)
        df_combined = df_combined.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        df_combined = df_combined[~df_combined['id_cus'].isin([
            'FIN-TestApp', 'FIN-TestApp2', 'FIN-TestApp3', 'FIN-TestApp6', 'FIN-TestApp-2025', 'FNGtest', '????♡☆umata♡☆??'
        ])]

        df_combined['model'] = df_combined['model'].apply(lambda val: re.sub(r'^[\W_]+', '', str(val).strip()) if pd.notnull(val) else None)
        
        # Convert transaction_date to YYYYMMDD int
        df_combined['transaction_date'] = pd.to_datetime(df_combined['transaction_date'], errors='coerce')
        df_combined['transaction_date'] = df_combined['transaction_date'].dt.strftime('%Y%m%d').astype('Int64')
        df_combined = df_combined.drop_duplicates()

        logger.info(f"✅ Cleaned data shape: {df_combined.shape}")
        return df_combined
    
    except Exception as e:
        logger.error(f"❌ Error in clean_fact_check_price: {str(e)}")
        raise

@op
def load_fact_check_price(df: pd.DataFrame):
    """Load data with better memory management and streaming approach"""
    try:
        table_name = 'fact_check_price'
        pk_column = ['id_cus', 'brand', 'model', 'submodel', 'yearcar', 'car_code',
                     'sum_insured', 'type_camera', 'type_addon', 'transaction_date',
                     'province_car', 'company_name1', 'company_name2', 'company_name3',
                     'company_name4', 'selecteds', 'type_insurance', 'id_car']

        logger.info(f"💾 Loading data - Input shape: {df.shape}")

        # ✅ กรองซ้ำจาก DataFrame ใหม่
        df = df[~df[pk_column].duplicated(keep='first')].copy()
        logger.info(f"✅ After deduplication: {df.shape}")

        # ✅ Load ข้อมูลเดิมจาก PostgreSQL แบบ streaming
        logger.info("📊 Loading existing data from PostgreSQL...")
        existing_keys = set()
        
        # ใช้ query แยกเพื่อลด memory usage
        try:
            with target_engine.connect() as conn:
                # โหลดเฉพาะ primary keys ที่มีอยู่
                result = conn.execute(text(f"SELECT {', '.join(pk_column)} FROM {table_name}"))
                for row in result:
                    existing_keys.add(tuple(row))
        except SQLAlchemyError as e:
            logger.warning(f"⚠️ Could not load existing data: {str(e)}")
            existing_keys = set()
        
        logger.info(f"✅ Found {len(existing_keys)} existing records")

        # ✅ สร้าง tuple key สำหรับเปรียบเทียบ
        df['pk_tuple'] = df[pk_column].apply(lambda row: tuple(row), axis=1)

        # ✅ หาข้อมูลใหม่ที่ยังไม่มีในฐานข้อมูล
        new_keys = set(df['pk_tuple']) - existing_keys
        df_to_insert = df[df['pk_tuple'].isin(new_keys)].copy()

        # ✅ หาข้อมูลที่มีอยู่แล้ว และเปรียบเทียบว่าเปลี่ยนแปลงหรือไม่
        common_keys = set(df['pk_tuple']) & existing_keys
        df_common_new = df[df['pk_tuple'].isin(common_keys)].copy()

        # โหลดข้อมูลเดิมเฉพาะที่ต้องเปรียบเทียบ
        df_diff = pd.DataFrame()
        if not df_common_new.empty and len(common_keys) <= 1000:  # จำกัดจำนวน records ที่เปรียบเทียบ
            logger.info("📊 Loading existing data for comparison...")
            try:
                pk_conditions = []
                for key in list(common_keys)[:1000]:  # จำกัดจำนวน conditions
                    conditions = []
                    for i, col in enumerate(pk_column):
                        conditions.append(f"{col} = '{key[i]}'")
                    pk_conditions.append(f"({' AND '.join(conditions)})")
                
                where_clause = " OR ".join(pk_conditions[:100])  # จำกัดจำนวน conditions
                if len(pk_conditions) > 100:
                    logger.warning(f"⚠️ Limiting comparison to first 100 records due to query size")
                
                with target_engine.connect() as conn:
                    df_common_old = pd.read_sql(f"SELECT * FROM {table_name} WHERE {where_clause}", conn)
                
                if not df_common_old.empty:
                    # ✅ แปลง dtype ให้ตรงกันระหว่าง df และ df_existing
                    for col in pk_column:
                        if col in df_common_new.columns and col in df_common_old.columns:
                            try:
                                df_common_new[col] = df_common_new[col].astype(df_common_old[col].dtype)
                            except Exception:
                                df_common_new[col] = df_common_new[col].astype(str)
                                df_common_old[col] = df_common_old[col].astype(str)

                    df_common_new['pk_tuple'] = df_common_new[pk_column].apply(lambda row: tuple(row), axis=1)
                    df_common_old['pk_tuple'] = df_common_old[pk_column].apply(lambda row: tuple(row), axis=1)

                    df_common_new.set_index(pk_column, inplace=True)
                    df_common_old.set_index(pk_column, inplace=True)

                    df_common_new = df_common_new.sort_index()
                    df_common_old = df_common_old.sort_index()

                    df_diff_mask = ~(df_common_new.eq(df_common_old, axis=1).all(axis=1))
                    df_diff = df_common_new[df_diff_mask].reset_index()
                    
                    del df_common_old  # Free memory
                    gc.collect()
            except SQLAlchemyError as e:
                logger.warning(f"⚠️ Could not compare existing data: {str(e)}")
                df_diff = pd.DataFrame()

        logger.info(f"🆕 Insert: {len(df_to_insert)} rows")
        logger.info(f"🔄 Update: {len(df_diff)} rows")

        # ✅ Load table metadata
        metadata = Table(table_name, MetaData(), autoload_with=target_engine)

        # ✅ Insert
        if not df_to_insert.empty:
            df_to_insert = df_to_insert.drop(columns=['pk_tuple'])
            df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna().all(axis=1)].copy()
            dropped = len(df_to_insert) - len(df_to_insert_valid)
            if dropped > 0:
                logger.warning(f"⚠️ Skipped {dropped} rows with null primary keys")
            
            if not df_to_insert_valid.empty:
                logger.info("💾 Inserting new records...")
                # Insert แบบ batch เพื่อลด memory
                batch_size = 500  # ลด batch size
                for i in range(0, len(df_to_insert_valid), batch_size):
                    batch = df_to_insert_valid.iloc[i:i+batch_size]
                    try:
                        with target_engine.begin() as conn:
                            conn.execute(metadata.insert(), batch.to_dict(orient='records'))
                        logger.info(f"   Inserted batch {i//batch_size + 1}/{(len(df_to_insert_valid)-1)//batch_size + 1}")
                    except SQLAlchemyError as e:
                        logger.error(f"❌ Error inserting batch {i//batch_size + 1}: {str(e)}")
                        continue

        # ✅ Update
        if not df_diff.empty:
            logger.info("💾 Updating existing records...")
            try:
                with target_engine.begin() as conn:
                    for i, record in enumerate(df_diff.to_dict(orient='records')):
                        stmt = pg_insert(metadata).values(**record)
                        update_columns = {
                            c.name: stmt.excluded[c.name]
                            for c in metadata.columns
                            if c.name not in pk_column + ['check_price_id', 'create_at', 'update_at']
                        }
                        # update_at ให้เป็นเวลาปัจจุบัน
                        update_columns['update_at'] = datetime.now()
                        stmt = stmt.on_conflict_do_update(
                            index_elements=pk_column,
                            set_=update_columns
                        )
                        conn.execute(stmt)
                        
                        if (i + 1) % 50 == 0:  # ลดความถี่ของ logging
                            logger.info(f"   Updated {i + 1}/{len(df_diff)} records")
            except SQLAlchemyError as e:
                logger.error(f"❌ Error updating records: {str(e)}")

        # Free memory
        del df, df_to_insert, df_diff, existing_keys
        gc.collect()

        logger.info("✅ Insert/update completed.")
    
    except SQLAlchemyError as e:
        logger.error(f"❌ Database error in load_fact_check_price: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Error in load_fact_check_price: {str(e)}")
        raise

@job
def fact_check_price_etl():
    load_fact_check_price(clean_fact_check_price(extract_fact_check_price()))

if __name__ == "__main__":
    df_logs, df_checkprice = extract_fact_check_price()
    # print("✅ Extracted logs:", df_logs.shape)
    # print("✅ Extracted checkprice:", df_checkprice.shape)

    df_clean = clean_fact_check_price((df_logs, df_checkprice))
    # print("✅ Cleaned columns:", df_clean.columns)

    # print(df_clean.head(10))

    output_path = "fact_check_price.xlsx"
    df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")
#
#     load_fact_check_price(df_clean)
#     print("🎉 Test completed! Data upserted to dim_car.")
