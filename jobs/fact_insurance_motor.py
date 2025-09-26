from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, func, or_, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import OperationalError, DisconnectionError
from datetime import datetime, timedelta

# timezone helper
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# ✅ Load .env
load_dotenv()

# ✅ Source DB connections with timeout and retry settings
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_timeout=30,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300,
        'charset': 'utf8mb4',
        'autocommit': True
    }
)

task_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_timeout=30,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300,
        'charset': 'utf8mb4',
        'autocommit': True
    }
)

# ✅ Target PostgreSQL
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@"
    f"{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

# -------------------------
# 🔧 Utilities
# -------------------------

def _today_range_th():
    """คืนค่า (start_dt, end_dt) เป็น naive datetime ของช่วงวันนี้ตาม Asia/Bangkok"""
    if ZoneInfo:
        tz = ZoneInfo("Asia/Bangkok")
        now_th = datetime.now(tz)
        start_th = now_th.replace(hour=0, minute=0, second=0, microsecond=0)
        end_th = start_th + timedelta(days=1)
        return start_th.replace(tzinfo=None), end_th.replace(tzinfo=None)
    # fallback UTC+7
    now = datetime.utcnow() + timedelta(hours=7)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

def normalize_null_like_series(col: pd.Series) -> pd.Series:
    """แปลงค่า null-like เป็น pd.NA (case-insensitive)"""
    if col.dtype == object or pd.api.types.is_string_dtype(col):
        s = col.astype(str)
        mask = s.str.strip().str.lower().isin({'', 'null', 'none', 'nan', 'na', 'n/a', '-', 'ไม่มี', 'ไม่ระบุ'})
        return col.mask(mask, pd.NA)
    return col

def purge_na_tokens(df: pd.DataFrame) -> pd.DataFrame:
    """เคลียร์ค่า null-like สำหรับ export/โหลดเข้า DB"""
    token_set = {'<na>', 'na', 'n/a', 'null', 'none', 'nan', ''}
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            s = df[col].astype(str).str.strip()
            df[col] = df[col].where(~s.str.lower().isin(token_set), None)
    df = df.astype(object).where(pd.notnull(df), None)
    return df

def remove_commas_from_numeric(value):
    """ลบลูกน้ำออกจากค่าตัวเลข (ต่อ cell)"""
    if pd.isna(value) or value is None:
        return value
    value_str = str(value).strip().replace(',', '').strip()
    if value_str == '' or value_str.lower() in ['nan', 'none', 'null']:
        return None
    if re.match(r'^-?\d*\.?\d+$', value_str):
        return value_str
    return value

def is_numeric_like_series(s: pd.Series) -> pd.Series:
    s_str = s.astype(str).str.replace(',', '', regex=False).str.strip()
    return s_str.str.match(r'^-?\d+(\.\d+)?$', na=False)

def clean_numeric_commas_for_series(col: pd.Series) -> pd.Series:
    s = col.astype(str).str.replace(',', '', regex=False).str.strip()
    mask = s.str.match(r'^-?\d+(\.\d+)?$', na=False)
    out = col.copy()
    out[mask] = pd.to_numeric(s[mask], errors='coerce')
    out = normalize_null_like_series(out)
    return out

def auto_clean_numeric_like_columns(df: pd.DataFrame, exclude: set | None = None, threshold: float = 0.8, min_numeric_rows: int = 5) -> pd.DataFrame:
    exclude = exclude or set()
    for col in df.columns:
        if col in exclude:
            continue
        if not (df[col].dtype == object or pd.api.types.is_string_dtype(df[col])):
            continue
        s = df[col].astype(str).str.strip()
        thai_frac = s.str.contains(r'[ก-๙]', regex=True, na=False).mean()
        if thai_frac > 0.2:
            continue
        non_null_mask = ~s.str.strip().str.lower().isin({'', 'null', 'none', 'nan', 'na', 'n/a', '-', 'ไม่มี', 'ไม่ระบุ'})
        if non_null_mask.sum() == 0:
            continue
        numeric_like_mask = is_numeric_like_series(s[non_null_mask])
        numeric_like_ratio = numeric_like_mask.mean()
        numeric_like_count = numeric_like_mask.sum()
        if numeric_like_ratio >= threshold and numeric_like_count >= min_numeric_rows:
            df[col] = clean_numeric_commas_for_series(df[col])
    return df

def clean_insurance_company(company):
    """ทำความสะอาดชื่อบริษัทประกัน (คงไว้เฉพาะข้อความไทยที่ดูสมเหตุสมผล)"""
    if pd.isna(company) or company is None:
        return None
    company_str = str(company).strip()
    invalid_patterns = [
        r'[<>"\'\`\\]', r'\.\./', r'[\(\)\{\}\[\]]+', r'[!@#$%^&*+=|]+',
        r'XOR', r'if\(', r'now\(\)', r'\$\{', r'\?\?\?\?', r'[0-9]+[XO][0-9]+',
    ]
    for pattern in invalid_patterns:
        if re.search(pattern, company_str, re.IGNORECASE):
            return None
    if len(company_str) < 2 or len(company_str) > 100:
        return None
    if not re.search(r'[ก-๙]', company_str):
        return None
    cleaned_company = re.sub(r'[0-9a-zA-Z\s]+', ' ', company_str)
    cleaned_company = re.sub(r'\s+', ' ', cleaned_company).strip()
    if len(cleaned_company) < 2:
        return None
    if not re.search(r'[ก-๙]', cleaned_company):
        return None
    return cleaned_company

def execute_query_with_retry(engine, query, params=None, max_retries=3, delay=5):
    """Execute query with retry mechanism for connection issues (รองรับ params)"""
    for attempt in range(max_retries):
        try:
            print(f"🔄 Attempt {attempt + 1}/{max_retries} - Executing query...")
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params=params)
            print(f"✅ Query executed successfully on attempt {attempt + 1}")
            return df
        except (OperationalError, DisconnectionError) as e:
            print(f"⚠️ DB error on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                print(f"⏳ Waiting {delay} seconds before retry...")
                time.sleep(delay)
                delay *= 2
                engine.dispose()
            else:
                print("❌ All retry attempts failed")
                raise
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            raise

def close_engines():
    """Safely close all database engines"""
    try:
        source_engine.dispose()
        task_engine.dispose()
        target_engine.dispose()
        print("🔒 Database connections closed safely")
    except Exception as e:
        print(f"⚠️ Warning: Error closing connections: {str(e)}")

def parse_amount(value):
    """แปลงจำนวนเงินทนสตริง -> float/NaN"""
    if pd.isna(value):
        return np.nan
    s = str(value).strip().lower()
    if s in {"", "ฟรี", "free", "ยกเว้น", "ไม่มี", "ไม่คิด", "0", "0 บาท"}:
        return 0
    s = re.sub(r"[^0-9\.\-]", "", s)
    if s in {"", "-", "."}:
        return np.nan
    try:
        return float(s)
    except:
        return np.nan

# -------------------------
# 🧲 Extract
# -------------------------

@op
def extract_motor_data():

    start_dt = '2025-09-01 00:00:00'
    end_dt = '2025-09-31 25:59:59'

    # start_dt, end_dt = _today_range_th()
    print(f"⏱️ Extract window (TH): {start_dt} → {end_dt}")

    # ดึงจาก fin_system_select_plan เฉพาะวันนี้ด้วยพารามิเตอร์
    plan_query = text("""
        SELECT quo_num, company, company_prb, assured_insurance_capital1, is_addon, type, repair_type, prb
        FROM fin_system_select_plan
        WHERE datestart >= :start_dt AND datestart < :end_dt
          AND type_insure = 'ประกันรถ'
    """)
    df_plan = execute_query_with_retry(source_engine, plan_query, params={"start_dt": start_dt, "end_dt": end_dt})

    # ตารางอื่นเป็นข้อมูลอ้างอิง/ประกอบ (ไม่กรองเวลา) — หากต้องการกรอง เพิ่ม WHERE เหมือนกันได้
    order_query = text("""
        SELECT quo_num, responsibility1, responsibility2, responsibility3, responsibility4,
               damage1, damage2, damage3, damage4, protect1, protect2, protect3, protect4,
               IF(sendtype = 'ที่อยู่ใหม่', provincenew, province) AS delivery_province,
               show_ems_price, show_ems_type, sendtype
        FROM fin_order
    """)
    df_order = execute_query_with_retry(task_engine, order_query)

    pay_query = text("""
        SELECT quo_num, date_warranty, date_exp
        FROM fin_system_pay
    """)
    df_pay = execute_query_with_retry(source_engine, pay_query)

    print("📦 df_plan:", df_plan.shape)
    print("📦 df_order:", df_order.shape)
    print("📦 df_pay:", df_pay.shape)

    try:
        source_engine.dispose()
        task_engine.dispose()
        print("🔒 Source database connections closed")
    except Exception as e:
        print(f"⚠️ Warning: Error closing source connections: {str(e)}")

    return df_plan, df_order, df_pay

# -------------------------
# 🧼 Clean / Transform
# -------------------------

@op
def clean_motor_data(data_tuple):
    df_plan, df_order, df_pay = data_tuple

    df = df_plan.merge(df_order, on="quo_num", how="left")
    df = df.merge(df_pay, on="quo_num", how="left")

    df = df.rename(columns={
        "quo_num": "quotation_num",
        "company": "ins_company",
        "company_prb": "act_ins_company",
        "assured_insurance_capital1": "sum_insured",
        "is_addon": "income_comp_ins",
        "responsibility1": "human_coverage_person",
        "responsibility2": "human_coverage_atime",
        "responsibility3": "property_coverage",
        "responsibility4": "deductible",
        "damage1": "vehicle_damage",
        "damage2": "deductible_amount",
        "damage3": "vehicle_theft_fire",
        "damage4": "vehicle_flood_damage",
        "protect1": "personal_accident_driver",
        "protect2": "personal_accident_passengers",
        "protect3": "medical_coverage",
        "protect4": "driver_coverage",
        "show_ems_price": "ems_amount",
        "show_ems_type": "delivery_type",
        "date_warranty": "date_warranty",
        "date_exp": "date_expired",
        "type": "insurance_class"
    })

    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df["non_empty_count"] = df_temp.notnull().sum(axis=1)
    df = df.sort_values("non_empty_count", ascending=False).drop_duplicates(subset="quotation_num")
    df = df.drop(columns=["non_empty_count"], errors="ignore")

    df.columns = df.columns.str.lower()
    df["income_comp_ins"] = df["income_comp_ins"].apply(lambda x: True if x == 1 else False if x == 0 else None)

    # ✅ company fields
    if 'ins_company' in df.columns:
        before = df['ins_company'].notna().sum()
        df['ins_company'] = df['ins_company'].apply(clean_insurance_company)
        df['ins_company'] = normalize_null_like_series(df['ins_company'])
        after = df['ins_company'].notna().sum()
        print(f"🧹 Cleaned ins_company - filtered {before - after} invalid")

    if 'act_ins_company' in df.columns:
        before = df['act_ins_company'].notna().sum()
        df['act_ins_company'] = df['act_ins_company'].apply(clean_insurance_company)
        df['act_ins_company'] = normalize_null_like_series(df['act_ins_company'])
        after = df['act_ins_company'].notna().sum()
        print(f"🧹 Cleaned act_ins_company - filtered {before - after} invalid")

    # ✅ dates
    df["date_warranty"] = pd.to_datetime(df["date_warranty"], errors="coerce")
    df["date_expired"] = pd.to_datetime(df["date_expired"], errors="coerce")
    df["date_warranty"] = df["date_warranty"].replace({pd.NaT: None})
    df["date_expired"] = df["date_expired"].replace({pd.NaT: None})
    df['date_warranty'] = pd.to_datetime(df['date_warranty'], errors='coerce').dt.strftime('%Y%m%d').astype('Int64')
    df['date_expired'] = pd.to_datetime(df['date_expired'], errors='coerce').dt.strftime('%Y%m%d').astype('Int64')

    # ✅ province
    def clean_province(province):
        if pd.isna(province) or str(province).strip() == '':
            return None
        province_str = str(province).strip()
        for w in ['จังหวัด', 'อำเภอ', 'ตำบล', 'เขต', 'เมือง', 'กิโลเมตร', 'กม.', 'ถนน', 'ซอย', 'หมู่', 'บ้าน']:
            province_str = province_str.replace(w, '').strip()
        corrections = {
            'กรุงเทพ': 'กรุงเทพมหานคร','กรุงเทพฯ': 'กรุงเทพมหานคร','กทม': 'กรุงเทพมหานคร','กทม.': 'กรุงเทพมหานคร',
            'พัทยา': 'ชลบุรี','ศรีราชา': 'ชลบุรี',
            'บางนา': 'สมุทรปราการ','บางพลี': 'สมุทรปราการ','พระประแดง': 'สมุทรปราการ','บางบ่อ': 'สมุทรปราการ','บางเสาธง': 'สมุทรปราการ'
        }
        for wrong, correct in corrections.items():
            if wrong in province_str:
                return correct
        known = [
            'กรุงเทพมหานคร','เชียงใหม่','ชลบุรี','นนทบุรี','ปทุมธานี','สมุทรปราการ','สมุทรสาคร','นครปฐม','นครราชสีมา',
            'ขอนแก่น','อุบลราชธานี','สุราษฎร์ธานี','สงขลา','ภูเก็ต','เชียงราย','ลำปาง','ลำพูน','แพร่','น่าน','พะเยา',
            'แม่ฮ่องสอน','ตาก','สุโขทัย','พิษณุโลก','เพชรบูรณ์','พิจิตร','กำแพงเพชร','อุทัยธานี','นครสวรรค์','ลพบุรี',
            'สิงห์บุรี','ชัยนาท','สระบุรี','พระนครศรีอยุธยา','อ่างทอง','สุพรรณบุรี','นครนายก','สระแก้ว','จันทบุรี','ตราด',
            'ฉะเชิงเทรา','ปราจีนบุรี'
        ]
        for k in known:
            if k in province_str or province_str in k:
                return k
        return None

    if 'delivery_province' in df.columns:
        df['delivery_province'] = df['delivery_province'].apply(clean_province)
        print("🧹 Cleaned delivery_province")

    # ✅ delivery_type
    if 'delivery_type' in df.columns:
        df['delivery_type'] = df['delivery_type'].replace({'nor': 'normal', 'ระบบเดิม': 'normal', 'ปกติ': 'normal'})

    # ✅ insurance_class
    if 'insurance_class' in df.columns:
        valid = ['1', '1+', '2', '2+', '3', '3+', 'พรบ']
        before = df['insurance_class'].notna().sum()
        df['insurance_class'] = df['insurance_class'].astype(str).str.strip()
        df['insurance_class'] = df['insurance_class'].apply(lambda x: x if x in valid else None)
        print(f"🧹 Cleaned insurance_class - filtered {before - df['insurance_class'].notna().sum()} invalid")

    # ✅ repair_type
    if 'repair_type' in df.columns:
        def clean_repair_type(val):
            if pd.isna(val) or val is None:
                return None
            s = str(val).strip()
            thai_only = re.sub(r'[^ก-๙]', '', s)
            if len(thai_only) < 2:
                return None
            if thai_only in ['อู่','ซ่อมอู้','ซ่อมอู๋']:
                return 'ซ่อมอู่'
            elif thai_only in ['ซ่องห้าง','ซ่อมศูนย์']:
                return 'ซ่อมห้าง'
            return thai_only
        before = df['repair_type'].notna().sum()
        df['repair_type'] = df['repair_type'].apply(clean_repair_type)
        print(f"🧹 Cleaned repair_type - filtered {before - df['repair_type'].notna().sum()} invalid")

    # ---------- NEW: fill type column from prb ----------
    def fill_type_from_prb(row):
        if pd.notnull(row.get('insurance_class')) and str(row['insurance_class']).strip() != "":
            return row['insurance_class']
        prb_val = str(row.get('prb')).lower() if row.get('prb') is not None else ""
        if "yes" in prb_val:  
            return "พรบ"
        return row.get('insurance_class')

    if 'insurance_class' in df.columns and 'prb' in df.columns:
        df['insurance_class'] = df.apply(fill_type_from_prb, axis=1)

    df = df.drop(columns=["prb"], errors="ignore")

    numeric_columns = [
        "sum_insured","human_coverage_person","human_coverage_atime","property_coverage",
        "deductible","vehicle_damage","deductible_amount","vehicle_theft_fire",
        "vehicle_flood_damage","personal_accident_driver","personal_accident_passengers",
        "medical_coverage","driver_coverage"
    ]
    print("🧹 Removing commas from known numeric columns...")
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(remove_commas_from_numeric)
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # ✅ ems_amount (พิเศษ)
    if 'ems_amount' in df.columns:
        df['ems_amount'] = df['ems_amount'].apply(parse_amount)
        df['ems_amount'] = pd.to_numeric(df['ems_amount'], errors='coerce').fillna(0).round().astype('Int64')
        print(f"📦 Cleaned ems_amount - non-null count: {df['ems_amount'].notna().sum()}")

    # ✅ fix outliers
    if 'human_coverage_atime' in df.columns:
        df['human_coverage_atime'] = df['human_coverage_atime'].replace(100000000, 10000000)
    if 'vehicle_damage' in df.columns:
        df['vehicle_damage'] = df['vehicle_damage'].replace(190000050, 1900000)
    if 'vehicle_theft_fire' in df.columns:
        df['vehicle_theft_fire'] = df['vehicle_theft_fire'].replace(190000050, 1900000)
    if 'sum_insured' in df.columns:
        df['sum_insured'] = df['sum_insured'].replace({190000050: 1900000, 250000093: 2500000})

    # ✅ ตรวจจับคอลัมน์ตัวเลขอัตโนมัติ (เฉพาะคอลัมน์ที่ส่วนใหญ่เป็นตัวเลขจริง) แล้วค่อยลบ comma/แปลง
    df = auto_clean_numeric_like_columns(
        df,
        exclude={'quotation_num', 'ins_company', 'act_ins_company', 'delivery_type', 'delivery_province', 'insurance_class', 'repair_type'},
        threshold=0.8,
        min_numeric_rows=5
    )

    # ✅ ready for DB/export
    df = df.convert_dtypes()
    df = purge_na_tokens(df)

    print("\n📊 Cleaning completed")

    try:
        source_engine.dispose()
        task_engine.dispose()
        print("🔒 Remaining database connections closed")
    except Exception as e:
        print(f"⚠️ Warning: Error closing remaining connections: {str(e)}")

    return df

# -------------------------
# 🚚 Load
# -------------------------
def chunker(df, size=5000):
    for i in range(0, len(df), size):
        yield df.iloc[i:i+size]

@op
def load_motor_data(df: pd.DataFrame):
    table_name = "fact_insurance_motor"
    pk_column = "quotation_num"

    # เอาเฉพาะแถวที่มี PK
    df = df[df[pk_column].notna()].copy()
    # กันซ้ำในชุดข้อมูลขาเข้าเองก่อน (ถ้าไม่มีคอลัมน์เวลา ใช้ keep='last')
    df = df.drop_duplicates(subset=[pk_column], keep='last')

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=target_engine)

    # ระบุคอลัมน์ที่อนุญาตให้อัปเดต (ยกเว้น PK และ audit columns)
    cols_to_update = [
        c.name for c in table.columns
        if c.name not in [pk_column, 'create_at', 'update_at']
    ]

    # ฟังก์ชันสร้าง UPSERT แบบอัปเดตเฉพาะเมื่อข้อมูล "ต่างจริง" (NULL-safe)
    def build_upsert_stmt(batch_records):
        insert_stmt = pg_insert(table).values(batch_records)

        # มีอย่างน้อย 1 คอลัมน์ที่ค่า != ของเดิม (NULL-safe ด้วย IS DISTINCT FROM)
        diff_conds = [
            table.c[col].is_distinct_from(insert_stmt.excluded[col])
            for col in cols_to_update
        ]
        where_any_diff = or_(*diff_conds)

        # อัปเดตค่าจาก excluded เฉพาะคอลัมน์ที่อนุญาต
        set_mapping = {col: insert_stmt.excluded[col] for col in cols_to_update}
        # เปลี่ยน update_at เฉพาะตอนเกิด UPDATE จริง (เพราะมี WHERE)
        set_mapping['update_at'] = func.now()

        # ถ้าต้องการอ้างอิงชื่อ unique constraint แทน index_elements ให้ใช้:
        # constraint="fact_insurance_motor_unique"
        return insert_stmt.on_conflict_do_update(
            index_elements=[pk_column],
            set_=set_mapping,
            where=where_any_diff
        )

    print("📤 Loading data to target database with conditional UPSERT...")

    try:
        with target_engine.begin() as conn:
            total = len(df)
            done = 0
            for batch in chunker(df, size=5000):
                records = batch.to_dict(orient="records")

                # ถ้าตารางไม่มี default ของ create_at ใน DB ให้เติมเองตอน INSERT
                if 'create_at' in table.c.keys():
                    for r in records:
                        if 'create_at' not in r or r['create_at'] in (None, ''):
                            r['create_at'] = func.now()

                stmt = build_upsert_stmt(records)
                conn.execute(stmt)

                done += len(batch)
                print(f"   ✅ upserted {done}/{total}")

        print("✅ UPSERT completed (insert new, update only when data changed)")
    finally:
        try:
            target_engine.dispose()
            print("🔒 Target database connection closed")
        except Exception as e:
            print(f"⚠️ Warning: Error closing target connection: {str(e)}")

        try:
            source_engine.dispose()
            task_engine.dispose()
            print("🔒 All database connections closed")
        except Exception as e:
            print(f"⚠️ Warning: Error closing all connections: {str(e)}")

# -------------------------
# 🧱 Dagster Job
# -------------------------

@job
def fact_insurance_motor_etl():
    load_motor_data(clean_motor_data(extract_motor_data()))

# -------------------------
# ▶️ Main (standalone run)
# -------------------------

if __name__ == "__main__":
    try:
        print("🚀 Starting fact_insurance_motor ETL process...")
        print("📥 Extracting data from source databases...")
        data_tuple = extract_motor_data()
        print("✅ Data extraction completed")

        print("🧹 Cleaning and transforming data...")
        df_clean = clean_motor_data(data_tuple)
        print("✅ Data cleaning completed")
        print("✅ Cleaned columns:", df_clean.columns)

        # output_path = "fact_insurance_motor.xlsx"
        # df_export = purge_na_tokens(df_clean.copy())
        # df_export.to_excel(output_path, index=False, engine='openpyxl')
        # print(f"💾 Saved to {output_path}")

        # โหลดเข้าฐาน (เปิดใช้เมื่อพร้อม)
        print("📤 Loading data to target database...")
        load_motor_data(df_clean)
        print("🎉 ETL process completed! Data upserted to fact_insurance_motor.")

    except Exception as e:
        print(f"❌ ETL process failed: {str(e)}")
        raise
    finally:
        close_engines()
