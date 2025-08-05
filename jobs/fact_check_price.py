from dagster import op, job
import pandas as pd
import numpy as np
import json
import re
import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, inspect, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import OperationalError, DisconnectionError
from datetime import datetime, timedelta

# ✅ Load .env
load_dotenv()

# ✅ DB source (MariaDB)
source_user = os.getenv('DB_USER')
source_password = os.getenv('DB_PASSWORD')
source_host = os.getenv('DB_HOST')
source_port = os.getenv('DB_PORT')
source_db = 'fininsurance'

source_engine = create_engine(
    f"mysql+pymysql://{source_user}:{source_password}@{source_host}:{source_port}/{source_db}"
)

# ✅ DB target (PostgreSQL)
target_user = os.getenv('DB_USER_test')
target_password = os.getenv('DB_PASSWORD_test')
target_host = os.getenv('DB_HOST_test')
target_port = os.getenv('DB_PORT_test')
target_db = 'fininsurance'

target_engine = create_engine(
    f"postgresql+psycopg2://{target_user}:{target_password}@{target_host}:{target_port}/{target_db}",
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={"connect_timeout": 30, "application_name": "fact_check_price_etl"}
)

def retry_db_operation(operation, max_retries=3, delay=2):
    for attempt in range(max_retries):
        try:
            return operation()
        except (OperationalError, DisconnectionError) as e:
            if attempt == max_retries - 1:
                raise e
            print(f"⚠️ DB error (attempt {attempt + 1}): {e}")
            time.sleep(delay)
            delay *= 2

@op
def extract_check_price_data() -> pd.DataFrame:
    now = datetime.now()

    start_time = now.replace(minute=0, second=0, microsecond=0)
    end_time = now.replace(minute=59, second=59, microsecond=999999)

    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    query_logs = """
    SELECT cuscode, brand, series, subseries, year, no_car, type, repair_type,
           assured_insurance_capital1, camera, addon, quo_num, create_at,
           results, selected, carprovince
    FROM fin_customer_logs_B2B
    WHERE create_at BETWEEN '{start_str}' AND '{end_str}'
    """
    query_checkprice = """
    SELECT id_cus, datekey, brand, model, submodel, yearcar, idcar, nocar,
           type_ins, company, tunprakan, deduct, status, type_driver,
           type_camera, type_addon, status_send
    FROM fin_checkprice
    WHERE datekey BETWEEN '{start_str}' AND '{end_str}'
    """
    df_logs = pd.read_sql(query_logs, source_engine)
    df_check = pd.read_sql(query_checkprice, source_engine)

    return pd.DataFrame({"logs": [df_logs], "check": [df_check]})

@op
def clean_check_price_data(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw['logs'][0].copy()
    df1 = raw['check'][0].copy()

    def extract_company_names(x):
        if pd.isnull(x):
            return [None, None, None, None]
        data = json.loads(x)
        names = []
        if isinstance(data, list):
            for d in data:
                if isinstance(d, dict):
                    names.append(d.get('company_name'))
                if len(names) == 4:
                    break
        elif isinstance(data, dict):
            names.append(data.get('company_name'))
        while len(names) < 4:
            names.append(None)
        return names[:4]

    company_names_df = df['results'].apply(extract_company_names).apply(pd.Series)
    company_names_df.columns = ['company_name1', 'company_name2', 'company_name3', 'company_name4']
    df = pd.concat([df.drop(columns=['results']), company_names_df], axis=1)

    def extract_selected_name(x):
        if pd.isnull(x):
            return None
        data = json.loads(x)
        if isinstance(data, list) and len(data) > 0:
            return data[0].get('company_name')
        elif isinstance(data, dict):
            return data.get('company_name')
        else:
            return None

    df['selecteds'] = df['selected'].apply(extract_selected_name)
    df.drop(columns=['selected'], inplace=True)

    df['camera'] = df['camera'].map({
        'yes': 'มีกล้องหน้ารถ',
        'no': 'ไม่มีกล้องหน้ารถ'
    })
    df['addon'] = df['addon'].map({
        'ไม่มี': 'ไม่มีการต่อเติม'
    })

    df.rename(columns={
        'cuscode': 'id_cus',
        'series': 'model',
        'subseries': 'submodel',
        'year': 'yearcar',
        'no_car': 'car_code',
        'assured_insurance_capital1': 'sum_insured',
        'camera': 'type_camera',
        'addon': 'type_addon',
        'quo_num': 'select_quotation',
        'create_at': 'transaction_date',
        'carprovince': 'province_car'
    }, inplace=True)

    df['type_insurance'] = 'ชั้น' + df['type'].astype(str) + df['repair_type'].astype(str)
    df.drop(columns=['type', 'repair_type'], inplace=True)
    df['input_type'] = 'auto'

    def split_company_names(x):
        if pd.isnull(x):
            return [None, None, None, None]
        names = [name.strip() for name in x.split('/')]
        while len(names) < 4:
            names.append(None)
        return names[:4]

    company_names_df = df1['company'].apply(split_company_names).apply(pd.Series)
    company_names_df.columns = ['company_name1', 'company_name2', 'company_name3', 'company_name4']
    df1 = pd.concat([df1.drop(columns=['company']), company_names_df], axis=1)

    province_list = [
        "กรุงเทพมหานคร", "กระบี่", "กาญจนบุรี", "กาฬสินธุ์", "กำแพงเพชร", "ขอนแก่น", "จันทบุรี", "ฉะเชิงเทรา",
        "ชลบุรี", "ชัยนาท", "ชัยภูมิ", "ชุมพร", "เชียงใหม่", "เชียงราย", "ตรัง", "ตราด", "ตาก", "นครนายก",
        "นครปฐม", "นครพนม", "นครราชสีมา", "นครศรีธรรมราช", "นครสวรรค์", "นนทบุรี", "นราธิวาส", "น่าน",
        "บึงกาฬ", "บุรีรัมย์", "ปทุมธานี", "ประจวบคีรีขันธ์", "ปราจีนบุรี", "ปัตตานี", "พระนครศรีอยุธยา",
        "พังงา", "พัทลุง", "พิจิตร", "พิษณุโลก", "เพชรบุรี", "เพชรบูรณ์", "แพร่", "พะเยา", "ภูเก็ต",
        "มหาสารคาม", "มุกดาหาร", "แม่ฮ่องสอน", "ยะลา", "ยโสธร", "ระนอง", "ระยอง", "ราชบุรี",
        "ร้อยเอ็ด", "ลพบุรี", "ลำปาง", "ลำพูน", "เลย", "ศรีสะเกษ", "สกลนคร", "สงขลา", "สตูล",
        "สมุทรปราการ", "สมุทรสงคราม", "สมุทรสาคร", "สระแก้ว", "สระบุรี", "สิงห์บุรี", "สุโขทัย",
        "สุพรรณบุรี", "สุราษฎร์ธานี", "สุรินทร์", "หนองคาย", "หนองบัวลำภู", "อ่างทอง", "อุดรธานี",
        "อุทัยธานี", "อุตรดิตถ์", "อุบลราชธานี", "อำนาจเจริญ"
    ]

    def extract_clean_plate(value):
        if pd.isnull(value) or value.strip() == "":
            return None
        text = value.strip()
        for prov in province_list:
            if prov in text:
                text = text.replace(prov, "").strip()
        if '//' in text:
            text = text.split('//')[0].strip()
        elif '/' in text:
            text = text.split('/')[0].strip()
        text_cleaned = text.replace('-', '').replace('/', '').replace(',', '').replace('+', '').replace(' ', '')
        text_cleaned = re.sub(r'^ป\d+และป\d+\+?', '', text_cleaned)
        text_cleaned = re.sub(r'^ป\d+\+?', '', text_cleaned)
        text_cleaned = re.sub(r'^งานป\d+', '', text_cleaned)
        pattern_plate = r'(\d{1}[ก-ฮ]{2}\d{1,4}|[ก-ฮ]{1,3}\d{1,4})'
        match_plate = re.match(pattern_plate, text_cleaned)
        if match_plate:
            plate = match_plate.group(1)
            if len(plate) <= 3:
                return None
            return plate
        return None

    def extract_province(value):
        if pd.isnull(value) or value.strip() == "":
            return None
        for prov in province_list:
            if prov in value:
                return prov
        return None

    df1['id_car'] = df1['idcar'].apply(extract_clean_plate)
    df1['province_car'] = df1['idcar'].apply(extract_province)
    df1.drop(columns=['idcar'], inplace=True)

    df1 = df1[~df1['id_cus'].isin(['ทศพรทดสอบ', 'ชัดเจนทดสอบ', 'FIN-TestApp'])]
    df1.rename(columns={
        'datekey': 'transaction_date',
        'nocar': 'car_code',
        'type_ins': 'type_insurance',
        'tunprakan': 'sum_insured',
        'deduct': 'deductible'
    }, inplace=True)
    df1['input_type'] = 'manual'

    df_combined = pd.concat([df, df1], ignore_index=True, sort=False)
    
    # แก้ไข deprecated methods - ใช้วิธีใหม่แทน
    # แทนที่ empty strings ด้วย NaN
    for col in df_combined.columns:
        if df_combined[col].dtype == 'object':
            df_combined[col] = df_combined[col].replace(r'^\s*$', np.nan, regex=True)
    
    df_combined = df_combined.where(pd.notnull(df_combined), None)
    
    # แก้ไข applymap เป็น apply
    for col in df_combined.columns:
        if df_combined[col].dtype == 'object':
            df_combined[col] = df_combined[col].apply(lambda x: x.lstrip() if isinstance(x, str) else x)
    
    df_combined = df_combined[~df_combined['id_cus'].isin([
        'FIN-TestApp', 'FIN-TestApp2', 'FIN-TestApp3',
        'FIN-TestApp6', 'FIN-TestApp-2025', 'FNGtest', '????♡☆umata☆??'
    ])]

    # ✅ แปลงวันที่แบบ vectorized
    date_columns = ['transaction_date']
    for col in date_columns:
        if col in df_combined.columns:
            # แปลงเป็น datetime ก่อน แล้วแปลงเป็น integer format YYYYMMDD
            df_combined[col] = pd.to_datetime(df_combined[col], errors='coerce')
            df_combined[col] = df_combined[col].dt.strftime('%Y%m%d').astype('Int64')

    # แปลง data types ให้ตรงกัน
    df_combined['yearcar'] = pd.to_numeric(df_combined['yearcar'], errors='coerce').astype('Int64')
    df_combined['sum_insured'] = pd.to_numeric(df_combined['sum_insured'], errors='coerce').astype('Int64')
    df_combined['deductible'] = pd.to_numeric(df_combined['deductible'], errors='coerce').astype('Int64')
    df_combined['id_cus'] = df_combined['id_cus'].astype(str)

    return df_combined

@op
def load_check_price_data(df: pd.DataFrame):
    try:
        table_name = 'fact_check_price'
        # ใช้ transaction_date เป็นตัวเทียบแทน composite key
        compare_column = 'transaction_date'

        print(f"📊 Processing {len(df)} rows...")
        
        # ตรวจสอบ data types ก่อน
        print("🔍 Data types before processing:")
        print(f"  {compare_column}: {df[compare_column].dtype}")

        # ✅ วันปัจจุบัน (เริ่มต้นเวลา 00:00:00)
        today_str = datetime.now().strftime('%Y-%m-%d')

        # ✅ Load เฉพาะข้อมูลที่อัปเดตวันนี้จาก PostgreSQL
        with target_engine.connect() as conn:
            df_existing = pd.read_sql(
                f"SELECT * FROM {table_name} WHERE update_at >= '{today_str}'",
                conn
            )

        print(f"📅 Found {len(df_existing)}")

        # ลบข้อมูลซ้ำในข้อมูลเดิม
        df_existing = df_existing[~df_existing.duplicated(keep='first')].copy()

        # หาข้อมูลใหม่ (ไม่มีใน database)
        existing_ids = set(df_existing[compare_column])
        new_ids = set(df[compare_column]) - existing_ids
        df_to_insert = df[df[compare_column].isin(new_ids)].copy()

        # หาข้อมูลที่มีอยู่แล้ว (มีใน database)
        common_ids = set(df[compare_column]) & existing_ids
        df_common_new = df[df[compare_column].isin(common_ids)].copy()
        df_common_old = df_existing[df_existing[compare_column].isin(common_ids)].copy()

        # รวมข้อมูลเพื่อเทียบ
        merged = df_common_new.merge(df_common_old, on=compare_column, suffixes=('_new', '_old'), how='inner')

        # คอลัมน์ที่ต้องเทียบ (ยกเว้น transaction_date และ metadata columns)
        exclude_columns = [compare_column, 'check_price_id', 'create_at', 'update_at']
        
        # หาคอลัมน์ที่เหมือนกันทั้ง df และ df_existing
        all_columns = set(df_common_new.columns) & set(df_common_old.columns)
        compare_cols = [
            col for col in all_columns
            if col not in exclude_columns
            and f"{col}_new" in merged.columns
            and f"{col}_old" in merged.columns
        ]

        def is_different(row):
            for col in compare_cols:
                val_new = row.get(f"{col}_new")
                val_old = row.get(f"{col}_old")
                if pd.isna(val_new) and pd.isna(val_old):
                    continue
                if val_new != val_old:
                    return True
            return False

        # กรองแถวที่มีการเปลี่ยนแปลง
        df_diff = merged[merged.apply(is_different, axis=1)].copy()

        if not df_diff.empty and compare_cols:
            update_cols = [f"{col}_new" for col in compare_cols]
            all_cols = [compare_column] + update_cols

            # เช็คให้ชัวร์ว่าคอลัมน์ที่เลือกมีจริง
            existing_cols = [c for c in all_cols if c in df_diff.columns]
            
            if len(existing_cols) > 1:  # ต้องมี compare_column และอย่างน้อย 1 คอลัมน์อื่น
                df_diff_renamed = df_diff.loc[:, existing_cols].copy()
                # เปลี่ยนชื่อ column ให้ตรงกับตารางจริง
                new_col_names = [compare_column] + [col.replace('_new', '') for col in existing_cols if col != compare_column]
                df_diff_renamed.columns = new_col_names
            else:
                df_diff_renamed = pd.DataFrame()
        else:
            df_diff_renamed = pd.DataFrame()

        print(f"🆕 Insert: {len(df_to_insert)} rows")
        print(f"🔄 Update: {len(df_diff_renamed)} rows")

        metadata = Table(table_name, MetaData(), autoload_with=target_engine)

        # Insert เฉพาะข้อมูลใหม่
        if not df_to_insert.empty:
            # แปลง NaN เป็น None สำหรับ PostgreSQL
            df_to_insert_valid = df_to_insert[df_to_insert[compare_column].notna()].copy()
            df_to_insert_valid = df_to_insert_valid.replace({np.nan: None})
            
            dropped = len(df_to_insert) - len(df_to_insert_valid)
            if dropped > 0:
                print(f"⚠️ Skipped {dropped} rows with null {compare_column}")
            if not df_to_insert_valid.empty:
                with target_engine.begin() as conn:
                    conn.execute(metadata.insert(), df_to_insert_valid.to_dict(orient='records'))
                print(f"✅ Inserted {len(df_to_insert_valid)} new records")

        # Update เฉพาะข้อมูลที่มีการเปลี่ยนแปลง
        if not df_diff_renamed.empty and compare_cols:
            # แปลง NaN เป็น None สำหรับ PostgreSQL
            df_diff_renamed = df_diff_renamed.replace({np.nan: None})
            
            with target_engine.begin() as conn:
                for record in df_diff_renamed.to_dict(orient='records'):
                    # ใช้ UPDATE statement แทน ON CONFLICT
                    transaction_date = record[compare_column]
                    
                    # สร้าง SET clause สำหรับคอลัมน์ที่ต้องอัปเดต
                    set_clause = []
                    update_values = {}
                    
                    for c in metadata.columns:
                        if c.name not in [compare_column, 'check_price_id', 'create_at', 'update_at']:
                            if c.name in record:
                                set_clause.append(f"{c.name} = %({c.name})s")
                                update_values[c.name] = record[c.name]
                    
                    # เพิ่ม update_at
                    set_clause.append("update_at = %(update_at)s")
                    update_values['update_at'] = datetime.now()
                    update_values['transaction_date'] = transaction_date
                    
                    # สร้าง UPDATE statement
                    update_sql = f"""
                    UPDATE {table_name} 
                    SET {', '.join(set_clause)}
                    WHERE {compare_column} = %(transaction_date)s
                    """
                    
                    conn.execute(text(update_sql), update_values)
            print(f"✅ Updated {len(df_diff_renamed)} records")

        print("✅ Insert/update completed.")
        
    except Exception as e:
        print(f"❌ Error in load_check_price_data: {str(e)}")
        print(f"🔍 Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        raise

@job
def fact_check_price_etl():
    load_check_price_data(clean_check_price_data(extract_check_price_data()))

if __name__ == "__main__":
    df_raw = extract_check_price_data()
    # print("✅ Extracted logs:", df_raw.shape)

    df_clean = clean_check_price_data(df_raw)
#     print("✅ Cleaned columns:", df_clean.columns)

    output_path = "fact_check_price.xlsx"
    df_clean.to_excel(output_path, index=False, engine='openpyxl')
    print(f"💾 Saved to {output_path}")

    load_check_price_data(df_clean)
    print("🎉 completed! Data upserted to fact_check_price.")
