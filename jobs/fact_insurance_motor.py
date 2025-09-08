from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import OperationalError, DisconnectionError
from datetime import datetime, timedelta

# ✅ Load .env
load_dotenv()

# ✅ Source DB connections with timeout and retry settings
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
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
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
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
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

# -------------------------
# 🔧 Utilities
# -------------------------

def normalize_null_like_series(col: pd.Series) -> pd.Series:
    """แปลงค่า null-like ('', 'NULL', 'None', 'NaN', 'N/A', '-', 'ไม่มี', 'ไม่ระบุ') เป็น pd.NA แบบ case-insensitive"""
    if col.dtype == object or pd.api.types.is_string_dtype(col):
        s = col.astype(str)
        mask = s.str.strip().str.lower().isin({
            '', 'null', 'none', 'nan', 'na', 'n/a', '-', 'ไม่มี', 'ไม่ระบุ'
        })
        return col.mask(mask, pd.NA)
    return col

def purge_na_tokens(df: pd.DataFrame) -> pd.DataFrame:
    """
    เคลียร์ค่า null-like ทุกแบบสำหรับ export/โหลดเข้า DB:
    - สตริง '<NA>', 'NA', 'N/A', 'NULL', 'None', 'NaN', '' -> None (case-insensitive, trim)
    - pd.NA/NaN -> None
    """
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
    """
    คืน mask ว่า cell ดูเป็นตัวเลขหรือไม่ (หลังลบ comma และ trim)
    """
    s_str = s.astype(str).str.replace(',', '', regex=False).str.strip()
    return s_str.str.match(r'^-?\d+(\.\d+)?$', na=False)

def clean_numeric_commas_for_series(col: pd.Series) -> pd.Series:
    """
    ลบ comma และแปลงเป็นตัวเลขเฉพาะตำแหน่งที่เป็น numeric-like
    ตำแหน่งอื่นคงค่าเดิม (ข้อความ)
    """
    s = col.astype(str).str.replace(',', '', regex=False).str.strip()
    mask = s.str.match(r'^-?\d+(\.\d+)?$', na=False)
    out = col.copy()
    out[mask] = pd.to_numeric(s[mask], errors='coerce')
    # ทำให้สตริงว่าง/คำว่า null-like เป็น NA
    out = normalize_null_like_series(out)
    return out

def auto_clean_numeric_like_columns(df: pd.DataFrame, exclude: set | None = None, threshold: float = 0.8, min_numeric_rows: int = 5) -> pd.DataFrame:
    """
    ตรวจจับคอลัมน์ข้อความที่ 'ส่วนใหญ่' เป็นตัวเลข แล้วลบ comma/แปลงเป็นตัวเลขเฉพาะ cell ที่ใช่
    - threshold: สัดส่วน (ในแถวที่ไม่ว่าง) ที่เป็นตัวเลข เช่น 0.8 = 80%
    - min_numeric_rows: จำนวนแถวขั้นต่ำที่เป็นตัวเลขเพื่อป้องกัน false positive
    """
    exclude = exclude or set()
    for col in df.columns:
        if col in exclude:
            continue
        if not (df[col].dtype == object or pd.api.types.is_string_dtype(df[col])):
            continue

        s = df[col].astype(str).str.strip()
        # ข้ามคอลัมน์ที่มีตัวอักษรไทยบ่อย (ไม่น่าใช่ตัวเลข)
        thai_frac = s.str.contains(r'[ก-๙]', regex=True, na=False).mean()
        if thai_frac > 0.2:
            continue

        # ประเมินเฉพาะแถวที่ไม่ว่าง/ไม่ใช่ null-like
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
    """ทำความสะอาดชื่อบริษัทประกัน: เก็บเฉพาะอักษรไทย, ตัดตัวเลข/อังกฤษ/สัญลักษณ์, กัน pattern แปลก"""
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

def execute_query_with_retry(engine, query, max_retries=3, delay=5):
    """Execute query with retry mechanism for connection issues"""
    for attempt in range(max_retries):
        try:
            print(f"🔄 Attempt {attempt + 1}/{max_retries} - Executing query...")
            df = pd.read_sql(query, engine)
            print(f"✅ Query executed successfully on attempt {attempt + 1}")
            return df
        except (OperationalError, DisconnectionError) as e:
            if "Lost connection" in str(e) or "connection was forcibly closed" in str(e):
                print(f"⚠️ Connection lost on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"⏳ Waiting {delay} seconds before retry...")
                    time.sleep(delay)
                    delay *= 2
                    engine.dispose()
                else:
                    print(f"❌ All retry attempts failed")
                    raise
            else:
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
    """แปลงจำนวนเงินทนสตริง: 'ฟรี', '0 บาท', '1,234', 'ระบบเดิม', None -> ตัวเลขหรือ NaN"""
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
    plan_query = """
        SELECT quo_num, company, company_prb, assured_insurance_capital1, is_addon, type, repair_type
        FROM fin_system_select_plan
        WHERE update_at BETWEEN '2025-01-01' AND '2025-08-31' 
          AND type_insure = 'ประกันรถ'
    """
    df_plan = execute_query_with_retry(source_engine, plan_query)

    order_query = """
        SELECT quo_num, responsibility1, responsibility2, responsibility3, responsibility4,
               damage1, damage2, damage3, damage4, protect1, protect2, protect3, protect4,
               IF(sendtype = 'ที่อยู่ใหม่', provincenew, province) AS delivery_province,
               show_ems_price, show_ems_type, sendtype
        FROM fin_order
    """
    df_order = execute_query_with_retry(task_engine, order_query)

    pay_query = """
        SELECT quo_num, date_warranty, date_exp
        FROM fin_system_pay
    """
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

    # ✅ numeric columns (known list - ชั้นที่ 1)
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

@op
def load_motor_data(df: pd.DataFrame):
    table_name = "fact_insurance_motor_temp"
    pk_column = "quotation_num"

    df = df[df[pk_column].notna()].copy()
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=target_engine)

    today_str = datetime.now().strftime('%Y-%m-%d')

    existing_query = f"SELECT {pk_column} FROM {table_name} WHERE update_at >= '{today_str}'"
    df_existing = execute_query_with_retry(target_engine, existing_query)
    existing_ids = set(df_existing[pk_column]) if not df_existing.empty else set()

    new_rows = df[~df[pk_column].isin(existing_ids)].copy()
    update_rows = df[df[pk_column].isin(existing_ids)].copy()

    print(f"🆕 Insert: {len(new_rows)} rows")
    print(f"🔄 Update: {len(update_rows)} rows")

    if not new_rows.empty:
        print(f"🔄 Inserting {len(new_rows)} new records...")
        try:
            with target_engine.begin() as conn:
                conn.execute(table.insert(), new_rows.to_dict(orient="records"))
            print(f"✅ Insert completed successfully")
        except Exception as e:
            print(f"❌ Insert failed: {str(e)}")
            raise

    if not update_rows.empty:
        print(f"🔄 Updating {len(update_rows)} existing records...")
        try:
            with target_engine.begin() as conn:
                for i, record in enumerate(update_rows.to_dict(orient="records")):
                    if i % 100 == 0:
                        print(f"   Progress: {i}/{len(update_rows)} records processed")
                    stmt = pg_insert(table).values(**record)
                    update_dict = {
                        c.name: stmt.excluded[c.name]
                        for c in table.columns
                        if c.name not in [pk_column, 'create_at', 'update_at']
                    }
                    update_dict['update_at'] = datetime.now()
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[pk_column],
                        set_=update_dict
                    )
                    conn.execute(stmt)
            print(f"✅ Update completed successfully")
        except Exception as e:
            print(f"❌ Update failed: {str(e)}")
            raise

    print("✅ Insert/update completed.")

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
