from dagster import op, job, schedule
import pandas as pd
import numpy as np
import re
import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import OperationalError, DisconnectionError
from datetime import datetime, timedelta
from sqlalchemy import or_, func

# timezone helper
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None

# ✅ Load .env
load_dotenv()

# ✅ DB source (MariaDB)
source_user = os.getenv('DB_USER')
source_password = os.getenv('DB_PASSWORD')
source_host = os.getenv('DB_HOST')
source_port = os.getenv('DB_PORT')
source_db = 'fininsurance'

source_engine = create_engine(
    f"mysql+pymysql://{source_user}:{source_password}@{source_host}:{source_port}/{source_db}",
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={"connect_timeout": 30}
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
    connect_args={"connect_timeout": 30, "application_name": "dim_car_etl"}
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
            delay *= 2

def _today_range_th():
    """Return naive datetimes [start, end) for 'today' in Asia/Bangkok."""
    if ZoneInfo:
        tz = ZoneInfo("Asia/Bangkok")
        now_th = datetime.now(tz)
        start_th = now_th.replace(hour=0, minute=0, second=0, microsecond=0)
        end_th = start_th + timedelta(days=1)
        # ส่งเป็น naive ให้ MySQL
        return start_th.replace(tzinfo=None), end_th.replace(tzinfo=None)
    # fallback UTC+7
    now = datetime.utcnow() + timedelta(hours=7)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

@op
def extract_car_data():

    start_dt = '2025-09-01 00:00:00'
    end_dt = '2025-10-31 25:59:59'

    # start_dt, end_dt = _today_range_th()
    print(f"⏱️ Extract window (TH): {start_dt} → {end_dt}")

    # ใช้พารามิเตอร์ ปลอดภัยกว่า
    query_pay = text("""
        SELECT quo_num,
               TRIM(id_motor1) AS id_motor1,
               TRIM(id_motor2) AS id_motor2,
               datestart
        FROM fin_system_pay
        WHERE datestart >= :start_dt AND datestart < :end_dt
        ORDER BY datestart DESC
    """)
    query_plan = text("""
        SELECT quo_num, idcar, carprovince, camera, no_car, brandplan, seriesplan, sub_seriesplan,
               yearplan, detail_car, vehGroup, vehBodyTypeDesc, seatingCapacity,
               weight_car, cc_car, color_car, datestart
        FROM fin_system_select_plan
        WHERE datestart >= :start_dt AND datestart < :end_dt
        ORDER BY datestart DESC
    """)

    try:
        with source_engine.connect() as conn:
            df_pay = pd.read_sql(query_pay, conn, params={"start_dt": start_dt, "end_dt": end_dt})
        print(f"📦 df_pay: {df_pay.shape}")
    except Exception as e:
        print(f"❌ ERROR querying fin_system_pay: {e}")
        df_pay = pd.DataFrame()

    try:
        with source_engine.connect() as conn:
            df_plan = pd.read_sql(query_plan, conn, params={"start_dt": start_dt, "end_dt": end_dt})
        print(f"📦 df_plan: {df_plan.shape}")
    except Exception as e:
        print(f"❌ ERROR querying fin_system_select_plan: {e}")
        df_plan = pd.DataFrame()

    if not df_pay.empty and not df_plan.empty:
        df_pay = df_pay.drop_duplicates(subset=['quo_num'], keep='first')
        df_plan = df_plan.drop_duplicates(subset=['quo_num'], keep='first')
        df_merged = pd.merge(df_pay, df_plan, on='quo_num', how='left')
        print(f"📊 After merge: {df_merged.shape}")
    elif not df_pay.empty:
        print("⚠️ Only fin_system_pay has data, using it alone")
        df_merged = df_pay.copy()
    elif not df_plan.empty:
        print("⚠️ Only fin_system_select_plan has data, using it alone")
        df_merged = df_plan.copy()
    else:
        print("❌ No data found in both tables for today")
        df_merged = pd.DataFrame()

    if not df_merged.empty:
        df_merged = df_merged.drop_duplicates()
        print(f"📊 After removing duplicates: {df_merged.shape}")

    if 'id_motor2' in df_merged.columns:
        valid_car_ids = df_merged['id_motor2'].notna().sum()
        print(f"✅ Valid car_ids: {valid_car_ids}/{len(df_merged)}")

    return df_merged

@op
def clean_car_data(df: pd.DataFrame):
    if df.empty:
        print("⚠️ WARNING: Input DataFrame is empty!")
        expected_columns = [
            'car_id', 'engine_number', 'car_registration',
            'car_province', 'camera', 'car_no', 'car_brand', 'car_series',
            'car_subseries', 'car_year', 'car_detail', 'vehicle_group',
            'vehbodytypedesc', 'seat_count', 'vehicle_weight', 'engine_capacity',
            'vehicle_color'
        ]
        return pd.DataFrame(columns=expected_columns)

    print(f"🔍 Starting data cleaning with {len(df)} records")

    required_cols = ['id_motor2', 'idcar', 'quo_num']
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        print(f"⚠️ WARNING: Missing required columns: {missing_cols}")
        if not any(col in df.columns for col in required_cols):
            print("❌ ERROR: No required columns found!")
            return pd.DataFrame()

    print(f"📊 Before removing duplicates: {df.shape}")
    if 'id_motor2' in df.columns:
        df = df.drop_duplicates(subset=['id_motor2'], keep='first')
        print(f"📊 After removing id_motor2 duplicates: {df.shape}")

    df = df.drop(columns=['datestart_x', 'datestart_y', 'quo_num'], errors='ignore')

    rename_columns = {
        "id_motor2": "car_vin",
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

    existing_columns = [c for c in rename_columns if c in df.columns]
    if existing_columns:
        df = df.rename(columns={c: rename_columns[c] for c in existing_columns})
        print(f"✅ Renamed {len(existing_columns)} columns")

    if 'car_vin' not in df.columns:
        print("⚠️ WARNING: car_vin column not found after renaming! Creating empty column.")
        df['car_vin'] = None

    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)

    valid_mask = df['car_vin'].astype(str).str.strip().ne('') & df['car_vin'].notna()
    df_with_id = df[valid_mask]
    df_without_id = df[~valid_mask]
    print(f"📊 Records with valid car_vin: {len(df_with_id)} / without: {len(df_without_id)}")

    df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='car_vin', keep='first')
    df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True).drop(columns=['non_empty_count'])
    df_cleaned.columns = df_cleaned.columns.str.lower()

    # ทำความสะอาดค่า
    province_list = ["กรุงเทพมหานคร","กระบี่","กาญจนบุรี","กาฬสินธุ์","กำแพงเพชร",
        "ขอนแก่น","จันทบุรี","ฉะเชิงเทรา","ชลบุรี","ชัยนาท","ชัยภูมิ",
        "ชุมพร","เชียงใหม่","เชียงราย","ตรัง","ตราด","ตาก","นครนายก",
        "นครปฐม","นครพนม","นครราชสีมา","นครศรีธรรมราช","นครสวรรค์",
        "นนทบุรี","นราธิวาส","น่าน","บึงกาฬ","บุรีรัมย์","ปทุมธานี",
        "ประจวบคีรีขันธ์","ปราจีนบุรี","ปัตตานี","พระนครศรีอยุธยา",
        "พังงา","พัทลุง","พิจิตร","พิษณุโลก","เพชรบุรี","เพชรบูรณ์",
        "แพร่","พะเยา","ภูเก็ต","มหาสารคาม","มุกดาหาร","แม่ฮ่องสอน",
        "ยะลา","ยโสธร","ระนอง","ระยอง","ราชบุรี","ร้อยเอ็ด","ลพบุรี",
        "ลำปาง","ลำพูน","เลย","ศรีสะเกษ","สกลนคร","สงขลา","สตูล",
        "สมุทรปราการ","สมุทรสงคราม","สมุทรสาคร","สระแก้ว","สระบุรี",
        "สิงห์บุรี","สุโขทัย","สุพรรณบุรี","สุราษฎร์ธานี","สุรินทร์",
        "หนองคาย","หนองบัวลำภู","อ่างทอง","อุดรธานี","อุทัยธานี",
        "อุตรดิตถ์","อุบลราชธานี","อำนาจเจริญ"]

    # ------------- NEW: car_vin cleaning (strip '*' rules) -------------
    def clean_car_vin(v):
        if pd.isnull(v):
            return None
        s = str(v).strip()
        # ถ้ามีแต่ * (หนึ่งตัวหรือหลายตัว) → None
        if re.fullmatch(r'\*+', s):
            return None
        # ตัด * ออกทั้งหมด แล้วคืนค่าที่เหลือ
        s = s.replace('*', '').strip()
        return s or None

    if 'car_vin' in df_cleaned.columns:
        df_cleaned['car_vin'] = df_cleaned['car_vin'].apply(clean_car_vin)

    # ฟังก์ชันเดิม
    def extract_clean_plate(value):
        if pd.isnull(value) or str(value).strip() == "":
            return None
        try:
            parts = re.split(r'[\/]', str(value).strip())
            if not parts or not parts[0]:
                return None
            words = parts[0].split()
            if not words:
                return None
            textv = words[0]
            for prov in province_list:
                if prov in textv:
                    textv = textv.replace(prov, "").strip()
            reg_match = re.match(r'^((?:\d{1,2})?[ก-ฮ]{1,3}\d{1,4})', textv)
            if reg_match:
                final_plate = reg_match.group(1).replace('-', '')
                match_two_digits = re.match(r'^(\d{2})([ก-ฮ].*)$', final_plate)
                if match_two_digits:
                    final_plate = match_two_digits.group(1)[1:] + match_two_digits.group(2)
                if final_plate.startswith("0"):
                    final_plate = final_plate[1:]
                return final_plate
            return None
        except (IndexError, AttributeError, TypeError):
            return None

    pattern_to_none = r'^[-\.]+$|^(?i:none|nan|undefine|undefined)$'
    df_cleaned = df_cleaned.replace(pattern_to_none, np.nan, regex=True)

    def clean_engine_number(v):
        if pd.isnull(v):
            return None
        cleaned = re.sub(r'[^A-Za-z0-9]', '', str(v))
        return cleaned if cleaned else None

    if 'engine_number' in df_cleaned.columns:
        df_cleaned['engine_number'] = df_cleaned['engine_number'].apply(clean_engine_number)

    def has_thai_chars(v):
        if pd.isnull(v):
            return False
        return bool(re.search(r'[ก-๙]', str(v)))

    # ตัด car_vin ที่มีตัวไทย (หลังจากล้าง * แล้ว)
    df_cleaned = df_cleaned[~df_cleaned['car_vin'].apply(has_thai_chars)]

    # ---------- NEW: province vs plate routing on car_province ----------
    province_set = set(province_list)

    def extract_province_only(value):
        """คืนชื่อจังหวัดที่พบ (กรณีไม่มีทะเบียนในช่องนี้)"""
        if pd.isnull(value):
            return None
        textv = str(value).strip()
        if textv == "":
            return None
        for prov in province_list:
            if prov in textv:
                return prov
        return None

    def split_province_and_plate(value):
        """
        คืน (province, plate_from_here)
        - ถ้าพบทะเบียนในช่อง province: (None, plate)
        - ถ้าไม่พบทะเบียน: (province_only_or_None, None)
        """
        if pd.isnull(value) or str(value).strip() == "":
            return None, None
        textv = str(value).strip()
        plate = extract_clean_plate(textv)
        if plate:
            return None, plate
        else:
            return extract_province_only(textv), None

    if 'car_province' in df_cleaned.columns:
        tmp = df_cleaned['car_province'].apply(
            lambda v: pd.Series(split_province_and_plate(v), index=['_prov_clean', '_plate_move'])
        )
        df_cleaned = pd.concat([df_cleaned, tmp], axis=1)

        if 'car_registration' not in df_cleaned.columns:
            df_cleaned['car_registration'] = None

        # ย้ายทะเบียนจาก province → car_registration เฉพาะแถวที่ยังว่าง
        need_move = df_cleaned['_plate_move'].notna() & df_cleaned['car_registration'].isna()
        df_cleaned.loc[need_move, 'car_registration'] = df_cleaned.loc[need_move, '_plate_move']

        # พบทะเบียนใน province → ตั้ง province=None
        df_cleaned.loc[df_cleaned['_plate_move'].notna(), 'car_province'] = None

        # ไม่มีทะเบียน → เก็บเฉพาะชื่อจังหวัด
        mask_no_plate = df_cleaned['_plate_move'].isna()
        df_cleaned.loc[mask_no_plate, 'car_province'] = df_cleaned.loc[mask_no_plate, '_prov_clean']

        df_cleaned = df_cleaned.drop(columns=['_prov_clean', '_plate_move'])

    # ---------- NEW: clean car_registration (remove '-' and spaces) ----------
    def clean_car_registration(v):
        if pd.isnull(v):
            return None
        s = str(v).strip()
        if not s:
            return None
        # ลบ - และช่องว่างทุกตำแหน่ง
        s = s.replace('-', '').replace(' ', '')
        return s or None

    if 'car_registration' in df_cleaned.columns:
        df_cleaned['car_registration'] = df_cleaned['car_registration'].apply(clean_car_registration)

    # -------------------------------------------------------------------

    # ล้าง space / ตัวพิเศษทุกคอลัมน์ string
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':
            df_cleaned[col] = df_cleaned[col].map(lambda x: x.strip() if isinstance(x, str) else x)
            df_cleaned[col] = df_cleaned[col].map(lambda x: re.sub(r'^[\s\-–_\.\/\+"\']+', '', str(x)) if isinstance(x, str) else x)
            df_cleaned[col] = df_cleaned[col].map(lambda x: re.sub(r'\s+', ' ', str(x)).strip() if isinstance(x, str) else x)

    def remove_leading_vowels(v):
        if pd.isnull(v): return v
        v = str(v).strip()
        v = re.sub(r"^[\u0E30-\u0E39\u0E47-\u0E4E\u0E3A\s\-_\.\/\+]+", "", v)
        v = re.sub(r'\s+', ' ', v)
        return v.strip()

    def translate_car_brand(v):
        if pd.isnull(v): return None
        v = str(v).strip()
        brand_translations = {
            'ยามาฮ่า': 'Yamaha','ฮอนด้า': 'Honda','เวสป้า': 'Vespa','คาวาซากิ': 'Kawasaki',
            'Kavasaki': 'Kawasaki','Mitsubushi Fuso': 'Mitsubishi Fuso','Peugeot': 'Peugeot',
            'Roya Enfield': 'Royal Enfield','Ssang Yong': 'SsangYong','Stallions': 'Stallion',
            'Takano': 'Tadano','Toyata':'Toyota','Zontes':'Zonetes','บีเอ็มดับบลิว': 'BMW',
            'B.M.W': 'BMW','totota': 'Toyota','ไทเกอร์': 'Tiger','FORO': 'Ford','FORD': 'Ford',
            'ฮาร์เลย์ เดวิดสัน': 'Harley Davidson','Alfaromeo': 'Alfa Romeo',
            '-':'ไม่ระบุ','–':'ไม่ระบุ','N/A':'ไม่ระบุ'
        }
        for th,en in brand_translations.items():
            if th.lower() in v.lower():
                return en
        return v

    def format_car_name(v):
        if pd.isnull(v): return None
        v = str(v).strip()
        if v in ['', 'nan', 'None', 'NULL', 'undefined', 'UNDEFINED']:
            return None
        v = re.sub(r'^[\s\_\.\/\+"\']+', '', v)
        v = re.sub(r'[\s\_\.\/\+"\']+$', '', v)
        if not v: return None
        v = v.lower()
        v = re.sub(r'([a-z])([A-Z])', r'\1 \2', v)
        v = re.sub(r'([a-z])([a-z])([A-Z])', r'\1\2 \3', v)
        v = v.title()
        if len(v) <= 3 and v.isalpha():
            return v.upper()
        return v

    if 'car_series' in df_cleaned.columns:
        df_cleaned['car_series'] = df_cleaned['car_series'].apply(remove_leading_vowels).apply(translate_car_brand).apply(format_car_name)
    if 'car_subseries' in df_cleaned.columns:
        df_cleaned['car_subseries'] = df_cleaned['car_subseries'].apply(remove_leading_vowels).apply(translate_car_brand).apply(format_car_name)
    if 'car_brand' in df_cleaned.columns:
        df_cleaned['car_brand'] = df_cleaned['car_brand'].apply(remove_leading_vowels).apply(translate_car_brand).apply(format_car_name)

    def clean_vehbodytypedesc_thai(v):
        if pd.isnull(v): return None
        v = str(v).strip()
        if v in ['', 'nan', 'None', 'NULL', 'undefined', 'UNDEFINED']: return None
        v = re.sub(r'^[\s\_\.\/\+"\']+', '', v)
        v = re.sub(r'[\s\_\.\/\+"\']+$', '', v).strip()
        if not v or not re.search(r'[ก-๙]', v): return None
        v = re.sub(r'[a-zA-Z0-9]', '', v)
        v = re.sub(r'\s+', ' ', v).strip()
        return v or None

    if 'vehbodytypedesc' in df_cleaned.columns:
        df_cleaned['vehbodytypedesc'] = df_cleaned['vehbodytypedesc'].apply(clean_vehbodytypedesc_thai)

    def clean_float_only(v):
        if pd.isnull(v): return None
        v = str(v).strip()
        v = re.sub(r'\.{2,}', '.', v)
        if v in ['', '.', '-']: return None
        try: return float(v)
        except ValueError: return None

    for c in ['engine_capacity', 'vehicle_weight']:
        if c in df_cleaned.columns:
            df_cleaned[c] = df_cleaned[c].apply(clean_float_only)
    if 'seat_count' in df_cleaned.columns:
        df_cleaned['seat_count'] = df_cleaned['seat_count'].apply(lambda x: int(clean_float_only(x)) if clean_float_only(x) is not None else None)
        df_cleaned['seat_count'] = pd.to_numeric(df_cleaned['seat_count'], errors='coerce').astype('Int64')
    if 'car_year' in df_cleaned.columns:
        df_cleaned['car_year'] = df_cleaned['car_year'].apply(lambda x: int(clean_float_only(x)) if clean_float_only(x) is not None else None)
        df_cleaned['car_year'] = pd.to_numeric(df_cleaned['car_year'], errors='coerce').astype('Int64')

    df_cleaned = df_cleaned.replace(r'NaN', np.nan, regex=True).drop_duplicates()
    if 'car_vin' in df_cleaned.columns:
        dups = df_cleaned['car_vin'].duplicated()
        if dups.any():
            print(f"⚠️ WARNING: Found {dups.sum()} duplicate car_vins after cleaning! Dropping dups.")
            df_cleaned = df_cleaned.drop_duplicates(subset=['car_vin'], keep='first')

    if 'car_vin' in df_cleaned.columns:
        nan_cnt = df_cleaned['car_vin'].isna().sum()
        if nan_cnt > 0:
            df_cleaned = df_cleaned[df_cleaned['car_vin'].notna()].copy()
            print(f"✅ Removed {nan_cnt} records with NaN car_vin")
    else:
        df_cleaned['car_vin'] = None

    print(f"📊 Final cleaned data shape: {df_cleaned.shape}")
    print(f"✅ Records with valid car_vin: {df_cleaned['car_vin'].notna().sum()}/{len(df_cleaned)}")
    df_cleaned = df_cleaned.drop_duplicates(subset=['car_vin'], keep='first')
    print(f"📊 Final records after removing car_vin duplicates: {len(df_cleaned)}")
    return df_cleaned

# ---------- UPSERT helper (เพิ่ม debug log) ----------
def upsert_batches(table, rows, key_col, update_cols, batch_size=10000):
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        try:
            print(f"🔎 Preparing batch {i//batch_size + 1}: {len(batch)} rows | update_cols={update_cols}")
            if not update_cols:
                print("⚠️ No update_cols found, skipping batch!")
                continue

            with target_engine.begin() as conn:
                ins = pg_insert(table).values(batch)
                excluded = ins.excluded  

                set_map = {
                    c: func.coalesce(getattr(excluded, c), getattr(table.c, c))
                    for c in update_cols
                }

                if 'update_at' in table.c:
                    set_map['update_at'] = func.now()

                change_conditions = [
                    func.coalesce(getattr(excluded, c), getattr(table.c, c)).is_distinct_from(getattr(table.c, c))
                    for c in update_cols
                ]

                stmt = ins.on_conflict_do_update(
                    index_elements=[table.c[key_col]],
                    set_=set_map,
                    where=or_(*change_conditions)
                )
                conn.execute(stmt)

            print(f"✅ Upserted batch {i//batch_size + 1}/{(len(rows)+batch_size-1)//batch_size} ({len(batch)} rows)")
        except Exception as e:
            print(f"❌ Upsert batch {i//batch_size + 1} failed: {e}")

@op
def load_car_data(df: pd.DataFrame):
    table_name = 'dim_car'
    pk_column = 'car_vin'

    if df.empty:
        print("⚠️ WARNING: Input DataFrame is empty! Skipping DB ops")
        return

    if pk_column not in df.columns:
        print(f"⚠️ WARNING: Column '{pk_column}' not found in DataFrame! Skipping DB ops")
        return

    df = df[~df[pk_column].duplicated(keep='first')].copy()
    df = df[df[pk_column].notna()].copy()

    print(f"🔎 Data prepared for upsert: {len(df)} rows")
    print("🔎 Sample car_vin:", df[pk_column].head(5).tolist())

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=target_engine)

    exclude_cols = {pk_column, 'car_id', 'create_at', 'datestart', 'update_at'}
    update_cols = [c for c in df.columns if c not in exclude_cols]

    print("🔎 Final update_cols:", update_cols)

    rows_to_upsert = df.replace({np.nan: None}).to_dict(orient='records')

    if rows_to_upsert:
        # ✅ นับ insert/update ล่วงหน้า
        with target_engine.connect() as conn:
            existing_keys = set(
                r[0] for r in conn.execute(
                    text(f"SELECT {pk_column} FROM {table_name} WHERE {pk_column} = ANY(:vals)"),
                    {"vals": list(df[pk_column].unique())}
                ).fetchall()
            )
        total = len(df)
        inserts = total - len(existing_keys)
        updates = len(existing_keys)

        print(f"📊 Pre-check → total: {total}, new inserts: {inserts}, possible updates: {updates}")

        # ✅ ดำเนินการ upsert
        upsert_batches(table, rows_to_upsert, pk_column, update_cols, batch_size=10000)

        print(f"✅ Insert/Update completed (UPSERT only — no forcing) → "
              f"Inserted {inserts}, Updated {updates} (if changed)")
    else:
        print("ℹ️ Nothing to upsert")

@job
def dim_car_etl():
    load_car_data(clean_car_data(extract_car_data()))

if __name__ == "__main__":
    df_raw = extract_car_data()
    print("✅ Extracted data shape:", df_raw.shape)

    if not df_raw.empty:
        df_clean = clean_car_data(df_raw)
        print("✅ Cleaned data shape:", df_clean.shape)
        print("✅ Cleaned columns:", list(df_clean.columns))

        # output_path = "dim_car.csv"
        # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
        # print(f"💾 Saved to {output_path}")

        # df_clean.to_excel("dim_car1.xlsx", index=False)
        # print("💾 Saved to dim_car.xlsx")

        load_car_data(df_clean)
        print("🎉 completed! Data to dim_car.")
    else:
        print("❌ No data extracted, skipping cleaning and saving")
