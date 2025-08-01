from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy import create_engine, MetaData, Table, inspect
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
    connect_args={
        "connect_timeout": 30,
        "application_name": "dim_car_etl"
    }
)

def retry_db_operation(operation, max_retries=3, delay=2):
    """Retry database operation with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return operation()
        except (OperationalError, DisconnectionError) as e:
            if attempt == max_retries - 1:
                raise e
            print(f"⚠️ Database connection error (attempt {attempt + 1}/{max_retries}): {e}")
            print(f"⏳ Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2  # Exponential backoff

@op
def extract_car_data():
    now = datetime.now()

    start_time = now.replace(minute=0, second=0, microsecond=0)
    end_time = now.replace(minute=59, second=59, microsecond=999999)

    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S') 

    query_pay = """
        SELECT quo_num, id_motor1, id_motor2, datestart
        FROM fin_system_pay
        WHERE datestart BETWEEN '{start_str}' AND '{end_str}'
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df_pay = pd.read_sql(query_pay, source_engine)

    query_plan = """
        SELECT quo_num, idcar, carprovince, camera, no_car, brandplan, seriesplan, sub_seriesplan,
               yearplan, detail_car, vehGroup, vehBodyTypeDesc, seatingCapacity,
               weight_car, cc_car, color_car, datestart
        FROM fin_system_select_plan
        WHERE datestart BETWEEN '{start_str}' AND '{end_str}'
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df_plan = pd.read_sql(query_plan, source_engine)

    df_merged = pd.merge(df_pay, df_plan, on='quo_num', how='left')
    
    # ✅ ตรวจสอบค่า NaN ในข้อมูลที่ extract
    print("🔍 NaN check in extracted data:")
    print(f"📦 df_pay shape: {df_pay.shape}")
    print(f"📦 df_plan shape: {df_plan.shape}")
    print(f"📦 df_merged shape: {df_merged.shape}")
    
    # ตรวจสอบ NaN ในคอลัมน์สำคัญ
    important_cols = ['id_motor2', 'idcar', 'quo_num']
    for col in important_cols:
        if col in df_merged.columns:
            nan_count = df_merged[col].isna().sum()
            if nan_count > 0:
                print(f"⚠️ {col}: {nan_count} NaN values")
            else:
                print(f"✅ {col}: No NaN values")
    
    return df_merged

@op
def clean_car_data(df: pd.DataFrame):
    df = df.drop_duplicates(subset=['id_motor2'])
    df = df.drop_duplicates(subset=['idcar'])
    df = df.drop(columns=['datestart_x', 'datestart_y'], errors='ignore')

    rename_columns = {
        "quo_num": "quotation_num",
        "id_motor2": "car_id",
        "id_motor1": "engine_number",
        "idcar": "car_registration",
        "carprovince": "car_province",
        "camera": "camera",
        "no_car": "car_no",
        "brandplan": "car_brand",
        "seriesplan": "car_series",
        "sub_seriesplan": "car_subseries",
        "yearplan": "car_year",
        "detail_car": "car_detail",
        "vehGroup": "vehicle_group",
        "vehBodyTypeDesc": "vehbodytypedesc",
        "seatingCapacity": "seat_count",
        "weight_car": "vehicle_weight",
        "cc_car": "engine_capacity",
        "color_car": "vehicle_color"
    }

    df = df.rename(columns=rename_columns)
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)

    valid_mask = df['car_id'].astype(str).str.strip().ne('') & df['car_id'].notna()
    df_with_id = df[valid_mask]
    df_without_id = df[~valid_mask]
    df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='car_id', keep='first')
    df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)
    df_cleaned = df_cleaned.drop(columns=['non_empty_count'])

    df_cleaned.columns = df_cleaned.columns.str.lower()

    df_cleaned['seat_count'] = df_cleaned['seat_count'].replace("อื่นๆ", np.nan)
    df_cleaned['seat_count'] = pd.to_numeric(df_cleaned['seat_count'], errors='coerce')

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

    def extract_clean_plate(value):
        if pd.isnull(value) or value.strip() == "":
            return None
        text = re.split(r'[\/]', value.strip())[0].split()[0]
        for prov in province_list:
            if prov in text:
                text = text.replace(prov, "").strip()
        reg_match = re.match(r'^((?:\d{1,2})?[ก-ฮ]{1,3}\d{1,4})', text)
        if reg_match:
            final_plate = reg_match.group(1).replace('-', '')
            match_two_digits = re.match(r'^(\d{2})([ก-ฮ].*)$', final_plate)
            if match_two_digits:
                final_plate = match_two_digits.group(1)[1:] + match_two_digits.group(2)
            if final_plate.startswith("0"):
                final_plate = final_plate[1:]
            return final_plate
        else:
            return None

    df_cleaned['car_registration'] = df_cleaned['car_registration'].apply(extract_clean_plate)

    # ✅ ปรับ pattern ใหม่: รองรับ "-", ".", "none", "NaN", "UNDEFINE", "undefined"
    pattern_to_none = r'^[-\.]+$|^(?i:none|nan|undefine|undefined)$'
    df_cleaned = df_cleaned.replace(pattern_to_none, np.nan, regex=True)

    # ✅ ทำความสะอาด engine_number ให้มีเฉพาะ A-Z, a-z, 0-9
    def clean_engine_number(value):
        if pd.isnull(value):
            return None
        cleaned = re.sub(r'[^A-Za-z0-9]', '', str(value))
        return cleaned if cleaned else None

    df_cleaned['engine_number'] = df_cleaned['engine_number'].apply(clean_engine_number)

    # ✅ ทำความสะอาด car_province ให้มีเฉพาะจังหวัด
    def clean_province(value):
        if pd.isnull(value):
            return None
        value = str(value).strip()
        if value in province_list:
            return value
        return None

    if 'car_province' in df_cleaned.columns:
        df_cleaned['car_province'] = df_cleaned['car_province'].apply(clean_province)
    else:
        print("⚠️ Column 'car_province' not found in DataFrame")
        
    df_cleaned = df_cleaned.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    if 'car_no' in df_cleaned.columns:
        df_cleaned['car_no'] = df_cleaned['car_no'].replace("ไม่มี", np.nan)
    else:
        print("⚠️ Column 'car_no' not found in DataFrame")
        
    if 'car_brand' in df_cleaned.columns:
        df_cleaned['car_brand'] = df_cleaned['car_brand'].replace("-", np.nan)

    def has_thai_chars(value):
        if pd.isnull(value):
            return False
        return bool(re.search(r'[ก-๙]', str(value)))

    df_cleaned = df_cleaned[~df_cleaned['car_id'].apply(has_thai_chars)]
    
    series_noise_pattern = r"^[-–_\.\/\+].*|^<=200CC$|^>250CC$|^'NQR 75$"

    # ✅ ตรวจสอบว่าคอลัมน์ car_series และ car_subseries มีอยู่หรือไม่
    if 'car_series' in df_cleaned.columns:
        df_cleaned['car_series'] = df_cleaned['car_series'].replace(series_noise_pattern, np.nan, regex=True)
    else:
        print("⚠️ Column 'car_series' not found in DataFrame")
        
    if 'car_subseries' in df_cleaned.columns:
        df_cleaned['car_subseries'] = df_cleaned['car_subseries'].replace(series_noise_pattern, np.nan, regex=True)
    else:
        print("⚠️ Column 'car_subseries' not found in DataFrame")

    def remove_leading_vowels(value):
        if pd.isnull(value):
            return value
        # ลบสระและเครื่องหมายกำกับไทยต้นข้อความ
        return re.sub(r"^[\u0E30-\u0E39\u0E47-\u0E4E\u0E3A]+", "", value.strip())

    if 'car_series' in df_cleaned.columns:
        df_cleaned['car_series'] = df_cleaned['car_series'].apply(remove_leading_vowels)
    if 'car_subseries' in df_cleaned.columns:
        df_cleaned['car_subseries'] = df_cleaned['car_subseries'].apply(remove_leading_vowels)
    
    # ✅ ทำความสะอาด car_brand - ลบสระและเครื่องหมายกำกับไทยที่อยู่ด้านหน้า
    if 'car_brand' in df_cleaned.columns:
        df_cleaned['car_brand'] = df_cleaned['car_brand'].apply(remove_leading_vowels)
    else:
        print("⚠️ Column 'car_brand' not found in DataFrame")
    
    # ✅ ทำความสะอาด car_brand เพิ่มเติม - ลบเครื่องหมายพิเศษและข้อมูลที่ไม่ต้องการ
    def clean_car_brand(value):
        if pd.isnull(value):
            return None
        
        value = str(value).strip()
        
        # ลบเครื่องหมายพิเศษที่อยู่ด้านหน้าและท้าย
        value = re.sub(r'^[-–_\.\/\+"\']+', '', value)  # ลบเครื่องหมายพิเศษด้านหน้า
        value = re.sub(r'[-–_\.\/\+"\']+$', '', value)  # ลบเครื่องหมายพิเศษด้านท้าย
        
        # ลบช่องว่างที่ซ้ำกัน
        value = re.sub(r'\s+', ' ', value)
        
        # ลบช่องว่างที่ต้นและท้าย
        value = value.strip()
        
        # ตรวจสอบว่าค่าที่เหลือมีความหมายหรือไม่
        if value in ['', 'nan', 'None', 'NULL', 'undefined', 'UNDEFINED']:
            return None
        
        return value
    
    if 'car_brand' in df_cleaned.columns:
        df_cleaned['car_brand'] = df_cleaned['car_brand'].apply(clean_car_brand)

        # ✅ Debug: แสดงตัวอย่างการทำความสะอาด car_brand
        print("🔍 Car brand cleaning examples:")
        sample_brands = df_cleaned['car_brand'].dropna().head(10)
        for brand in sample_brands:
            print(f"   - {brand}")
        
        # ✅ แสดงสถิติ car_brand
        brand_stats = df_cleaned['car_brand'].value_counts().head(10)
        print("🔍 Top 10 car brands:")
        for brand, count in brand_stats.items():
            print(f"   - {brand}: {count}")

    def clean_engine_capacity(value):
        if pd.isnull(value):
            return None
        value = str(value).strip()
        # ลบจุดซ้ำ เช่น "1..2" → "1.2"
        value = re.sub(r'\.{2,}', '.', value)
        # พยายามแปลงเป็น float
        try:
            float_val = float(value)
            return float_val
        except ValueError:
            return None

    if 'engine_capacity' in df_cleaned.columns:
        df_cleaned['engine_capacity'] = df_cleaned['engine_capacity'].apply(clean_engine_capacity)
    else:
        print("⚠️ Column 'engine_capacity' not found in DataFrame")

    def clean_float_only(value):
        if pd.isnull(value):
            return None
        value = str(value).strip()
        value = re.sub(r'\.{2,}', '.', value)
        if value in ['', '.', '-']:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    if 'engine_capacity' in df_cleaned.columns:
        df_cleaned['engine_capacity'] = df_cleaned['engine_capacity'].apply(clean_float_only)
    if 'vehicle_weight' in df_cleaned.columns:
        df_cleaned['vehicle_weight'] = df_cleaned['vehicle_weight'].apply(clean_float_only)
    else:
        print("⚠️ Column 'vehicle_weight' not found in DataFrame")
    if 'seat_count' in df_cleaned.columns:
        df_cleaned['seat_count'] = df_cleaned['seat_count'].apply(lambda x: int(clean_float_only(x)) if clean_float_only(x) is not None else None)
    else:
        print("⚠️ Column 'seat_count' not found in DataFrame")
    if 'car_year' in df_cleaned.columns:
        df_cleaned['car_year'] = df_cleaned['car_year'].apply(lambda x: int(clean_float_only(x)) if clean_float_only(x) is not None else None)
    else:
        print("⚠️ Column 'car_year' not found in DataFrame")

    if 'seat_count' in df_cleaned.columns:
        df_cleaned['seat_count'] = pd.to_numeric(df_cleaned['seat_count'], errors='coerce').astype('Int64')
    if 'car_year' in df_cleaned.columns:
        df_cleaned['car_year'] = pd.to_numeric(df_cleaned['car_year'], errors='coerce').astype('Int64')
    df_cleaned = df_cleaned.replace(r'NaN', np.nan, regex=True)
    df_cleaned = df_cleaned.drop_duplicates()

    # ✅ ตรวจสอบค่า NaN หลังการทำความสะอาด
    print("🔍 NaN check after cleaning:")
    final_nan_check = df_cleaned.isna().sum()
    final_nan_cols = final_nan_check[final_nan_check > 0]
    
    if len(final_nan_cols) > 0:
        print("⚠️ Columns with NaN after cleaning:")
        for col, count in final_nan_cols.items():
            percentage = (count / len(df_cleaned)) * 100
            print(f"   - {col}: {count} NaN values ({percentage:.2f}%)")
    else:
        print("✅ No NaN values found after cleaning")
    
    # ✅ ตรวจสอบ car_id ที่เป็น NaN (สำคัญที่สุด)
    if 'car_id' in df_cleaned.columns:
        car_id_nan = df_cleaned['car_id'].isna().sum()
        if car_id_nan > 0:
            print(f"⚠️ WARNING: {car_id_nan} records have NaN car_id")
            # ลบข้อมูลที่มี car_id เป็น NaN
            df_cleaned = df_cleaned[df_cleaned['car_id'].notna()].copy()
            print(f"✅ Removed {car_id_nan} records with NaN car_id")
    else:
        print("⚠️ Column 'car_id' not found in DataFrame (skip NaN check and removal)")
    
    print(f"📊 Final cleaned data shape: {df_cleaned.shape}")

    return df_cleaned

@op
def load_car_data(df: pd.DataFrame):
    table_name = 'dim_car'
    pk_column = 'car_id'

    # ✅ ตรวจสอบว่าตารางมี column 'quotation_num' หรือไม่ — ถ้าไม่มีก็สร้าง
    def check_and_add_column():
        with target_engine.connect() as conn:
            inspector = inspect(conn)
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            if 'quotation_num' not in columns:
                print("➕ Adding missing column 'quotation_num' to dim_car")
                conn.execute(f'ALTER TABLE {table_name} ADD COLUMN quotation_num VARCHAR')
                conn.commit()
    
    retry_db_operation(check_and_add_column)

    # ✅ กรอง car_id ซ้ำจาก DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()

    # ✅ ตรวจสอบค่า NaN ในข้อมูลก่อนเข้า database
    print("🔍 Checking for NaN values before database insertion:")
    nan_check = df.isna().sum()
    columns_with_nan = nan_check[nan_check > 0]
    
    if len(columns_with_nan) > 0:
        print("⚠️ Columns with NaN values:")
        for col, count in columns_with_nan.items():
            print(f"   - {col}: {count} NaN values")
        
        # แสดงตัวอย่างข้อมูลที่มี NaN
        print("🔍 Sample records with NaN values:")
        for col in columns_with_nan.index:
            sample_nan = df[df[col].isna()][[pk_column, col]].head(3)
            if not sample_nan.empty:
                print(f"   {col} NaN examples:")
                for _, row in sample_nan.iterrows():
                    print(f"     - {pk_column}: {row[pk_column]}, {col}: NaN")
    else:
        print("✅ No NaN values found in the data")
    
    # ✅ ตรวจสอบข้อมูลที่สำคัญ (car_id ไม่ควรเป็น NaN)
    critical_nan = df[pk_column].isna().sum()
    if critical_nan > 0:
        print(f"⚠️ WARNING: {critical_nan} records have NaN in {pk_column} (primary key)")
        # ลบข้อมูลที่มี car_id เป็น NaN
        df = df[df[pk_column].notna()].copy()
        print(f"✅ Removed {critical_nan} records with NaN {pk_column}")
    
    print(f"📊 Final data shape after NaN check: {df.shape}")

    # ✅ วันปัจจุบัน (เริ่มต้นเวลา 00:00:00)
    today_str = datetime.now().strftime('%Y-%m-%d')

    # ✅ Load เฉพาะข้อมูลวันนี้จาก PostgreSQL
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(
            f"SELECT * FROM {table_name} WHERE update_at >= '{today_str}'",
            conn
        )

    # ✅ กรอง car_id ซ้ำจากข้อมูลเก่า
    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    # ✅ Identify car_id ใหม่ (ไม่มีใน DB)
    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    # ✅ Identify car_id ที่มีอยู่แล้ว
    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    # ✅ Merge ด้วย suffix (_new, _old)
    merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

    # ✅ ระบุคอลัมน์ที่ใช้เปรียบเทียบ (ยกเว้น key และ audit fields)
    exclude_columns = [pk_column, 'car_sk', 'create_at', 'update_at']
    compare_cols = [
        col for col in df.columns
        if col not in exclude_columns
        and f"{col}_new" in merged.columns
        and f"{col}_old" in merged.columns
    ]

    # ✅ ตรวจสอบว่ามีคอลัมน์ที่สามารถเปรียบเทียบได้หรือไม่
    if not compare_cols:
        print("⚠️ No comparable columns found for update")
        df_diff_renamed = pd.DataFrame()
    else:
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

        if not df_diff.empty:
            # ✅ เตรียม DataFrame สำหรับ update โดยใช้ car_id ปกติ (ไม่เติม _new)
            update_cols = [f"{col}_new" for col in compare_cols]
            all_cols = [pk_column] + update_cols

            # ✅ ตรวจสอบว่าคอลัมน์ที่ต้องการมีอยู่ใน df_diff หรือไม่
            available_cols = [col for col in all_cols if col in df_diff.columns]
            if len(available_cols) != len(all_cols):
                missing_cols = set(all_cols) - set(df_diff.columns)
                print(f"⚠️ Missing columns in df_diff: {missing_cols}")
                print(f"🔍 Available columns: {list(df_diff.columns)}")
            
            df_diff_renamed = df_diff[available_cols].copy()
            # ✅ เปลี่ยนชื่อ column ให้ตรงกับตารางจริง (ลบ _new ออก)
            new_column_names = [pk_column] + [col.replace('_new', '') for col in available_cols if col != pk_column]
            df_diff_renamed.columns = new_column_names
        else:
            df_diff_renamed = pd.DataFrame()

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

    # ✅ Load table metadata
    def load_table_metadata():
        return Table(table_name, MetaData(), autoload_with=target_engine)
    
    metadata = retry_db_operation(load_table_metadata)

    # ✅ Insert (กรอง car_id ที่เป็น NaN)
    if not df_to_insert.empty:
        # ✅ ตรวจสอบ NaN ในข้อมูลที่จะ Insert
        print("🔍 Checking NaN in data to insert:")
        insert_nan_check = df_to_insert.isna().sum()
        insert_nan_cols = insert_nan_check[insert_nan_check > 0]
        if len(insert_nan_cols) > 0:
            print("⚠️ Insert data has NaN in columns:")
            for col, count in insert_nan_cols.items():
                print(f"   - {col}: {count} NaN values")
        
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} records with NaN {pk_column}")
        if not df_to_insert_valid.empty:
            # ✅ แทนที่ NaN ด้วย None ก่อนส่งไปยังฐานข้อมูล
            df_to_insert_clean = df_to_insert_valid.replace({np.nan: None})
            
            def insert_operation():
                with target_engine.begin() as conn:
                    # ✅ แบ่งข้อมูลเป็น batch เล็กๆ เพื่อลดภาระ
                    batch_size = 5000
                    records = df_to_insert_clean.to_dict(orient='records')
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        conn.execute(metadata.insert(), batch)
                        print(f"✅ Inserted batch {i//batch_size + 1}/{(len(records) + batch_size - 1)//batch_size}")
            
            retry_db_operation(insert_operation)

    # ✅ Update
    if not df_diff_renamed.empty:
        # ✅ ตรวจสอบ NaN ในข้อมูลที่จะ Update
        print("🔍 Checking NaN in data to update:")
        update_nan_check = df_diff_renamed.isna().sum()
        update_nan_cols = update_nan_check[update_nan_check > 0]
        if len(update_nan_cols) > 0:
            print("⚠️ Update data has NaN in columns:")
            for col, count in update_nan_cols.items():
                print(f"   - {col}: {count} NaN values")
        
        # ✅ แทนที่ NaN ด้วย None ก่อนส่งไปยังฐานข้อมูล
        df_diff_renamed_clean = df_diff_renamed.replace({np.nan: None})
        
        def update_operation():
            with target_engine.begin() as conn:
                for record in df_diff_renamed_clean.to_dict(orient='records'):
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
        
        retry_db_operation(update_operation)

    print("✅ Insert/update completed.")

@job
def dim_car_etl():
    load_car_data(clean_car_data(extract_car_data()))

if __name__ == "__main__":
    df_raw = extract_car_data()
    print("✅ Extracted logs:", df_raw.shape)

    df_clean = clean_car_data((df_raw))
    print("✅ Cleaned columns:", df_clean.columns)

    output_path = "dim_car.xlsx"
    df_clean.to_excel(output_path, index=False, engine='openpyxl')
    print(f"💾 Saved to {output_path}")

#     load_car_data(df_clean)
#     print("🎉 Test completed! Data upserted to dim_car.")


