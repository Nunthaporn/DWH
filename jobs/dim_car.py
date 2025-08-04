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

    start_time = now - timedelta(days=1)
    end_time = now

    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S') 

    print(f"🔍 Querying data from {start_str} to {end_str}")

    # ✅ ตรวจสอบการเชื่อมต่อฐานข้อมูลก่อน
    try:
        with source_engine.connect() as conn:
            # ตรวจสอบว่าตารางมีอยู่หรือไม่
            inspector = inspect(conn)
            tables = inspector.get_table_names()
            print(f"🔍 Available tables: {tables}")
            
            if 'fin_system_pay' not in tables:
                print("❌ ERROR: Table 'fin_system_pay' not found!")
                return pd.DataFrame()
            if 'fin_system_select_plan' not in tables:
                print("❌ ERROR: Table 'fin_system_select_plan' not found!")
                return pd.DataFrame()
            
            # ตรวจสอบจำนวนข้อมูลในตาราง
            count_pay = conn.execute(text("SELECT COUNT(*) FROM fin_system_pay")).scalar()
            count_plan = conn.execute(text("SELECT COUNT(*) FROM fin_system_select_plan")).scalar()
            print(f"📊 Total records in fin_system_pay: {count_pay}")
            print(f"📊 Total records in fin_system_select_plan: {count_plan}")
            
            # ตรวจสอบช่วงวันที่ที่มีข้อมูล
            date_range_pay = conn.execute(text("SELECT MIN(datestart), MAX(datestart) FROM fin_system_pay")).fetchone()
            date_range_plan = conn.execute(text("SELECT MIN(datestart), MAX(datestart) FROM fin_system_select_plan")).fetchone()
            print(f"📅 fin_system_pay date range: {date_range_pay}")
            print(f"📅 fin_system_select_plan date range: {date_range_plan}")
            
    except Exception as e:
        print(f"❌ ERROR connecting to database: {e}")
        return pd.DataFrame()

    query_pay = """
        SELECT quo_num, id_motor1, id_motor2, datestart
        FROM fin_system_pay
        WHERE datestart BETWEEN '{start_str}' AND '{end_str}'
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    
    # ✅ ลอง query โดยไม่จำกัดเวลาเพื่อดูว่ามีข้อมูลหรือไม่
    query_pay_test = """
        SELECT quo_num, id_motor1, id_motor2, datestart
        FROM fin_system_pay
        WHERE type_insure IN ('ประกันรถ', 'ตรอ')
        LIMIT 10
    """
    
    try:
        df_pay = pd.read_sql(query_pay, source_engine)
        print(f"📦 df_pay with date filter: {df_pay.shape}")
        
        if df_pay.empty:
            print("⚠️ No data found with date filter, trying without date filter...")
            df_pay = pd.read_sql(query_pay_test, source_engine)
            print(f"📦 df_pay without date filter: {df_pay.shape}")
            
            if not df_pay.empty:
                print("✅ Found data without date filter, using all available data")
                # ใช้ข้อมูลทั้งหมดที่มี
                query_pay_all = """
                    SELECT quo_num, id_motor1, id_motor2, datestart
                    FROM fin_system_pay
                    WHERE type_insure IN ('ประกันรถ', 'ตรอ')
                """
                df_pay = pd.read_sql(query_pay_all, source_engine)
                print(f"📦 df_pay all data: {df_pay.shape}")
    except Exception as e:
        print(f"❌ ERROR querying fin_system_pay: {e}")
        df_pay = pd.DataFrame()

    query_plan = """
        SELECT quo_num, idcar, carprovince, camera, no_car, brandplan, seriesplan, sub_seriesplan,
               yearplan, detail_car, vehGroup, vehBodyTypeDesc, seatingCapacity,
               weight_car, cc_car, color_car, datestart
        FROM fin_system_select_plan
        WHERE datestart BETWEEN '{start_str}' AND '{end_str}'
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    
    query_plan_test = """
        SELECT quo_num, idcar, carprovince, camera, no_car, brandplan, seriesplan, sub_seriesplan,
               yearplan, detail_car, vehGroup, vehBodyTypeDesc, seatingCapacity,
               weight_car, cc_car, color_car, datestart
        FROM fin_system_select_plan
        WHERE type_insure IN ('ประกันรถ', 'ตรอ')
        LIMIT 10
    """
    
    try:
        df_plan = pd.read_sql(query_plan, source_engine)
        print(f"📦 df_plan with date filter: {df_plan.shape}")
        
        if df_plan.empty:
            print("⚠️ No data found with date filter, trying without date filter...")
            df_plan = pd.read_sql(query_plan_test, source_engine)
            print(f"📦 df_plan without date filter: {df_plan.shape}")
            
            if not df_plan.empty:
                print("✅ Found data without date filter, using all available data")
                # ใช้ข้อมูลทั้งหมดที่มี
                query_plan_all = """
                    SELECT quo_num, idcar, carprovince, camera, no_car, brandplan, seriesplan, sub_seriesplan,
                           yearplan, detail_car, vehGroup, vehBodyTypeDesc, seatingCapacity,
                           weight_car, cc_car, color_car, datestart
                    FROM fin_system_select_plan
                    WHERE type_insure IN ('ประกันรถ', 'ตรอ')
                """
                df_plan = pd.read_sql(query_plan_all, source_engine)
                print(f"📦 df_plan all data: {df_plan.shape}")
    except Exception as e:
        print(f"❌ ERROR querying fin_system_select_plan: {e}")
        df_plan = pd.DataFrame()

    # ✅ Merge ข้อมูล
    if not df_pay.empty and not df_plan.empty:
        df_merged = pd.merge(df_pay, df_plan, on='quo_num', how='left')
    elif not df_pay.empty:
        print("⚠️ Only fin_system_pay has data, using it alone")
        df_merged = df_pay.copy()
    elif not df_plan.empty:
        print("⚠️ Only fin_system_select_plan has data, using it alone")
        df_merged = df_plan.copy()
    else:
        print("❌ No data found in both tables")
        df_merged = pd.DataFrame()
    
    # ✅ ตรวจสอบค่า NaN ในข้อมูลที่ extract
    print("🔍 NaN check in extracted data:")
    print(f"📦 df_pay shape: {df_pay.shape}")
    print(f"📦 df_plan shape: {df_plan.shape}")
    print(f"📦 df_merged shape: {df_merged.shape}")
    
    # ✅ ตรวจสอบว่าคอลัมน์สำคัญมีอยู่หรือไม่
    print("🔍 Checking for required columns:")
    required_cols = ['id_motor2', 'idcar', 'quo_num']
    for col in required_cols:
        if col in df_merged.columns:
            nan_count = df_merged[col].isna().sum()
            total_count = len(df_merged)
            valid_count = total_count - nan_count
            print(f"✅ {col}: {valid_count}/{total_count} valid values ({nan_count} NaN)")
            
            if valid_count == 0:
                print(f"⚠️ WARNING: No valid values in {col}!")
                print(f"🔍 Sample values: {df_merged[col].head(10).tolist()}")
        else:
            print(f"❌ ERROR: Required column '{col}' not found!")
            print(f"🔍 Available columns: {list(df_merged.columns)}")
    
    # ✅ ตรวจสอบว่ามีข้อมูลที่จำเป็นสำหรับการประมวลผลหรือไม่
    if 'id_motor2' not in df_merged.columns or df_merged['id_motor2'].notna().sum() == 0:
        print("❌ ERROR: No valid id_motor2 data found!")
        print("🔍 This will cause car_id to be missing in the final output")
        print("🔍 Sample data from df_pay:")
        print(df_pay.head())
        print("🔍 Sample data from df_plan:")
        print(df_plan.head())
    
    return df_merged

@op
def clean_car_data(df: pd.DataFrame):
    # ✅ ตรวจสอบว่า DataFrame ว่างเปล่าหรือไม่
    if df.empty:
        print("⚠️ WARNING: Input DataFrame is empty!")
        print("🔍 Returning empty DataFrame with expected columns")
        # สร้าง DataFrame ว่างที่มีคอลัมน์ที่คาดหวัง
        expected_columns = [
            'quotation_num', 'car_id', 'engine_number', 'car_registration', 
            'car_province', 'camera', 'car_no', 'car_brand', 'car_series', 
            'car_subseries', 'car_year', 'car_detail', 'vehicle_group', 
            'vehbodytypedesc', 'seat_count', 'vehicle_weight', 'engine_capacity', 
            'vehicle_color'
        ]
        return pd.DataFrame(columns=expected_columns)
    
    print(f"🔍 Starting data cleaning with {len(df)} records")
    
    # ✅ ตรวจสอบว่าคอลัมน์ที่จำเป็นมีอยู่หรือไม่
    required_cols = ['id_motor2', 'idcar', 'quo_num']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"⚠️ WARNING: Missing required columns: {missing_cols}")
        print(f"🔍 Available columns: {list(df.columns)}")
        # ใช้คอลัมน์ที่มีอยู่เท่านั้น
        available_cols = [col for col in required_cols if col in df.columns]
        if not available_cols:
            print("❌ ERROR: No required columns found!")
            return pd.DataFrame()
    
    # ✅ ลบ duplicates เฉพาะคอลัมน์ที่มีอยู่
    if 'id_motor2' in df.columns:
        df = df.drop_duplicates(subset=['id_motor2'])
    if 'idcar' in df.columns:
        df = df.drop_duplicates(subset=['idcar'])
    
    # ✅ ลบคอลัมน์ datestart ที่ซ้ำ
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

    # ✅ เปลี่ยนชื่อเฉพาะคอลัมน์ที่มีอยู่
    existing_columns = [col for col in rename_columns.keys() if col in df.columns]
    if existing_columns:
        rename_dict = {col: rename_columns[col] for col in existing_columns}
        df = df.rename(columns=rename_dict)
        print(f"✅ Renamed {len(existing_columns)} columns: {list(rename_dict.values())}")
    else:
        print("⚠️ WARNING: No columns to rename found!")
        print(f"🔍 Available columns: {list(df.columns)}")
    
    # ✅ ตรวจสอบว่าการเปลี่ยนชื่อคอลัมน์ทำงานถูกต้อง
    print("🔍 Column renaming check:")
    if 'car_id' in df.columns:
        car_id_count = df['car_id'].notna().sum()
        print(f"✅ car_id column exists with {car_id_count} valid values")
    else:
        print("⚠️ WARNING: car_id column not found after renaming!")
        print(f"🔍 Available columns: {list(df.columns)}")
        # สร้างคอลัมน์ car_id ว่างถ้าไม่มี
        df['car_id'] = None
        print("➕ Created empty car_id column")
    
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
    
    # ✅ ตรวจสอบว่าคอลัมน์ car_id ยังคงอยู่หลังจากเปลี่ยนเป็นตัวพิมพ์เล็ก
    print("🔍 After converting columns to lowercase:")
    if 'car_id' in df_cleaned.columns:
        car_id_count = df_cleaned['car_id'].notna().sum()
        print(f"✅ car_id column exists with {car_id_count} valid values")
    else:
        print("⚠️ WARNING: car_id column not found after lowercase conversion!")
        print(f"🔍 Available columns: {list(df_cleaned.columns)}")
        # สร้างคอลัมน์ car_id ว่างถ้าไม่มี
        df_cleaned['car_id'] = None
        print("➕ Created empty car_id column after lowercase conversion")

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
        if pd.isnull(value) or str(value).strip() == "":
            return None
        
        try:
            # แบ่งด้วย / และเอาแค่ส่วนแรก
            parts = re.split(r'[\/]', str(value).strip())
            if not parts or not parts[0]:
                return None
            
            # แบ่งด้วยช่องว่างและเอาแค่ส่วนแรก
            words = parts[0].split()
            if not words:
                return None
            
            text = words[0]
            
            # ลบชื่อจังหวัดออก
            for prov in province_list:
                if prov in text:
                    text = text.replace(prov, "").strip()
            
            # ตรวจสอบรูปแบบทะเบียนรถ
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
        except (IndexError, AttributeError, TypeError) as e:
            # ถ้าเกิด error ให้ return None
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
        
    # แทนที่ applymap ที่ deprecated ด้วย map
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':
            df_cleaned[col] = df_cleaned[col].map(lambda x: x.strip() if isinstance(x, str) else x)
    
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
        total_records = len(df_cleaned)
        print(f"🔍 car_id status: {total_records - car_id_nan}/{total_records} valid records")
        
        if car_id_nan > 0:
            print(f"⚠️ WARNING: {car_id_nan} records have NaN car_id")
            # ลบข้อมูลที่มี car_id เป็น NaN
            df_cleaned = df_cleaned[df_cleaned['car_id'].notna()].copy()
            print(f"✅ Removed {car_id_nan} records with NaN car_id")
            print(f"📊 Remaining records: {len(df_cleaned)}")
            
            # ตรวจสอบว่ายังมีข้อมูลเหลืออยู่หรือไม่
            if len(df_cleaned) == 0:
                print("⚠️ WARNING: No records remaining after removing NaN car_id!")
                print("🔍 This means all records had NaN car_id values")
                # ส่งคืน DataFrame ว่างแทนที่จะ raise error
                return pd.DataFrame(columns=df_cleaned.columns)
        else:
            print("✅ All car_id values are valid")
    else:
        print("⚠️ WARNING: Column 'car_id' not found in DataFrame!")
        print(f"🔍 Available columns: {list(df_cleaned.columns)}")
        # สร้างคอลัมน์ car_id ว่าง
        df_cleaned['car_id'] = None
        print("➕ Created empty car_id column")
    
    print(f"📊 Final cleaned data shape: {df_cleaned.shape}")
    
    # ✅ ตรวจสอบว่าคอลัมน์ car_id ยังคงอยู่
    if 'car_id' not in df_cleaned.columns:
        print("⚠️ WARNING: Column 'car_id' is missing after cleaning!")
        print(f"🔍 Available columns: {list(df_cleaned.columns)}")
        # สร้างคอลัมน์ car_id ว่าง
        df_cleaned['car_id'] = None
        print("➕ Created empty car_id column after cleaning")
    
    # ✅ ตรวจสอบว่ามีข้อมูลใน car_id หรือไม่
    car_id_count = df_cleaned['car_id'].notna().sum()
    print(f"✅ Records with valid car_id: {car_id_count}/{len(df_cleaned)}")
    
    if car_id_count == 0:
        print("⚠️ WARNING: No valid car_id records found!")
        if len(df_cleaned) > 0:
            print("🔍 Sample of car_id values:")
            print(df_cleaned['car_id'].head(10))
        else:
            print("🔍 DataFrame is empty")

    return df_cleaned

@op
def load_car_data(df: pd.DataFrame):
    table_name = 'dim_car'
    pk_column = 'car_id'

    # ✅ ตรวจสอบว่า DataFrame ว่างเปล่าหรือไม่
    if df.empty:
        print("⚠️ WARNING: Input DataFrame is empty!")
        print("🔍 Skipping database operations")
        return
    
    # ✅ ตรวจสอบว่าคอลัมน์ car_id มีอยู่ใน DataFrame หรือไม่
    print(f"🔍 Available columns in DataFrame: {list(df.columns)}")
    print(f"🔍 DataFrame shape: {df.shape}")
    
    if pk_column not in df.columns:
        print(f"⚠️ WARNING: Column '{pk_column}' not found in DataFrame!")
        print(f"🔍 Available columns: {list(df.columns)}")
        print(f"📊 DataFrame info:")
        print(df.info())
        print("🔍 Skipping database operations due to missing primary key column")
        return
    
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
    
    # ✅ ตรวจสอบว่ายังมีข้อมูลเหลืออยู่หรือไม่
    if df.empty:
        print("⚠️ WARNING: No valid data remaining after NaN check!")
        print("🔍 Skipping database operations")
        return

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
        
        if df_to_insert_valid.empty:
            print("⚠️ WARNING: No valid data to insert after NaN check!")
            return
        
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
    else:
        print("ℹ️ No data to update")

    print("✅ Insert/update completed.")

@job
def dim_car_etl():
    load_car_data(clean_car_data(extract_car_data()))

# if __name__ == "__main__":
#     df_raw = extract_car_data()
#     print("✅ Extracted logs:", df_raw.shape)

#     df_clean = clean_car_data((df_raw))
#     print("✅ Cleaned columns:", df_clean.columns)

#     output_path = "dim_car.xlsx"
#     df_clean.to_excel(output_path, index=False, engine='openpyxl')
#     print(f"💾 Saved to {output_path}")

#     load_car_data(df_clean)
#     print("🎉 Test completed! Data upserted to dim_car.")


