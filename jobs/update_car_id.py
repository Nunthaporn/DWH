# %%
from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
import time
import random
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# ✅ Load .env
load_dotenv()

# ✅ DB connections
# Source: MariaDB
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600,
    connect_args={"connect_timeout": 30}
)

# Target: PostgreSQL
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600,
    connect_args={"connect_timeout": 30, "application_name": "dim_car_update"}
)

# ---------- Helpers ----------
def clean_engine_number(v):
    if pd.isnull(v):
        return None
    cleaned = re.sub(r'[^A-Za-z0-9]', '', str(v))
    return cleaned if cleaned else None

def has_thai_chars(v):
    if pd.isnull(v):
        return False
    return bool(re.search(r'[ก-๙]', str(v)))

def strip_and_collapse(s):
    if not isinstance(s, str):
        return s
    s = s.strip()
    s = re.sub(r'^[\s\-–_\.\/\+"\']+', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()

# ---------- EXTRACT ----------
@op
def extract_pay() -> pd.DataFrame:
    q = """
        SELECT quo_num, TRIM(id_motor1) AS id_motor1, TRIM(id_motor2) AS id_motor2
        FROM fin_system_pay
        -- WHERE datestart BETWEEN '2025-01-01' AND '2025-08-31'
    """
    with source_engine.connect() as conn:
        df = pd.read_sql(q, conn.connection)
    df = df.drop_duplicates(subset=['quo_num'], keep='first')
    print(f"📦 df_pay: {df.shape}")
    return df

@op
def extract_plan() -> pd.DataFrame:
    q = """
        SELECT quo_num
        FROM fin_system_select_plan
        ORDER BY datestart DESC
    """
    with source_engine.connect() as conn:
        df = pd.read_sql(q, conn.connection)
    df = df.drop_duplicates(subset=['quo_num'], keep='first')
    print(f"📦 df_plan: {df.shape}")
    return df

@op
def extract_dim_car() -> pd.DataFrame:
    with target_engine.connect() as conn:
        df = pd.read_sql(text("SELECT car_id, car_vin, engine_number FROM dim_car"), conn)
    print(f"📦 dim_car: {df.shape}")
    return df

@op
def extract_fsq_all_quotations() -> pd.DataFrame:
    with target_engine.connect() as conn:
        df = pd.read_sql(text("SELECT quotation_num FROM fact_sales_quotation"), conn)
    print(f"📦 fsq(quotations): {df.shape}")
    return df

# ---------- TRANSFORM ----------
@op
def build_candidate_car_mapping(df_pay: pd.DataFrame, df_plan: pd.DataFrame) -> pd.DataFrame:
    # join pay + plan (รักษาเฉพาะที่มีใน plan ก็ได้ เพราะเป็น workflow ของขาย)
    df_merged = pd.merge(df_pay, df_plan, on='quo_num', how='left')

    # rename
    rename_map = {"quo_num": "quotation_num", "id_motor2": "car_vin", "id_motor1": "engine_number"}
    present = {k: v for k, v in rename_map.items() if k in df_merged.columns}
    df = df_merged.rename(columns=present)

    # ensure columns exist
    if 'car_vin' not in df.columns:
        df['car_vin'] = None

    # metric for choosing best record per car_vin (จำนวน non-null)
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['__non_empty'] = df_temp.notnull().sum(axis=1)

    # split valid/invalid by car_vin
    valid_mask = df['car_vin'].astype(str).str.strip().ne('') & df['car_vin'].notna()
    df_with_id = df[valid_mask].copy()
    df_without_id = df[~valid_mask].copy()

    # เลือกแถวที่ “ข้อมูลแน่นกว่า” ต่อ car_vin
    df_with_id = (
        df_with_id.sort_values('__non_empty', ascending=False)
                  .drop_duplicates(subset='car_vin', keep='first')
    )
    df_cleaned = pd.concat([df_with_id, df_without_id], ignore_index=True).drop(columns=['__non_empty'])

    # lower-column names
    df_cleaned.columns = df_cleaned.columns.str.lower()

    # clean engine_number
    if 'engine_number' in df_cleaned.columns:
        df_cleaned['engine_number'] = df_cleaned['engine_number'].apply(clean_engine_number)

    # drop car_vin with Thai chars
    df_cleaned = df_cleaned[~df_cleaned['car_vin'].apply(has_thai_chars)]

    # strip & normalize strings
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':
            df_cleaned[col] = df_cleaned[col].map(lambda x: strip_and_collapse(x) if isinstance(x, str) else x)

    # drop NaN dups & enforce uniqueness on car_vin
    df_cleaned = df_cleaned.replace(r'NaN', np.nan, regex=True).drop_duplicates()
    if 'car_vin' in df_cleaned.columns:
        if df_cleaned['car_vin'].duplicated().any():
            dup_cnt = df_cleaned['car_vin'].duplicated().sum()
            print(f"⚠️ duplicate car_vin after cleaning = {dup_cnt}, keeping first.")
            df_cleaned = df_cleaned.drop_duplicates(subset=['car_vin'], keep='first')
        # remove rows with NaN car_vin
        nan_cnt = df_cleaned['car_vin'].isna().sum()
        if nan_cnt > 0:
            df_cleaned = df_cleaned[df_cleaned['car_vin'].notna()].copy()
            print(f"✅ Removed {nan_cnt} records with NaN car_vin")

    print(f"🧹 candidate mapping (quotation_num, car_vin, engine_number): {df_cleaned.shape}")
    return df_cleaned[['quotation_num', 'car_vin', 'engine_number']].drop_duplicates()

@op
def join_with_dim_car(df_candidate: pd.DataFrame, df_dim_car: pd.DataFrame) -> pd.DataFrame:
    # join ด้วย (car_vin, engine_number) เพื่อนำ car_id เข้ามา
    df_final = pd.merge(
        df_candidate,
        df_dim_car[['car_id', 'car_vin', 'engine_number']],
        on=['car_vin', 'engine_number'],
        how='inner'
    )

    # เตรียมให้เหลือเฉพาะคอลัมน์ที่จะอัปเดต
    df_final = df_final[['quotation_num', 'car_id']].drop_duplicates()

    # sanity clean
    df_final = df_final.replace(['None', 'none', 'nan', 'NaN', 'NaT', ''], pd.NA)
    for c in ['quotation_num', 'car_id']:
        df_final[c] = df_final[c].astype('string').str.strip()

    print(f"🔗 df_final (quotation_num, car_id): {df_final.shape}")
    return df_final

@op
def filter_existing_fsq(df_final: pd.DataFrame, df_fsq_all: pd.DataFrame) -> pd.DataFrame:
    # เหลือเฉพาะ quotation_num ที่อยู่ใน FSQ (กัน key ไม่ตรง)
    df = pd.merge(df_final, df_fsq_all, on='quotation_num', how='inner')
    # กันซ้ำต่อ quotation_num
    df = df.dropna(subset=['quotation_num', 'car_id']).drop_duplicates(subset=['quotation_num'])
    print(f"✅ update candidates after FSQ filtering: {df.shape}")
    return df

# ---------- LOAD (UPDATE) ----------
@op
def update_car_id(df_updates: pd.DataFrame) -> None:
    if df_updates.empty:
        print("ℹ️ No rows to update.")
        return

    with target_engine.begin() as conn:
        # ลดโอกาส hang/deadlock
        conn.exec_driver_sql("SET lock_timeout = '3s'")
        conn.exec_driver_sql("SET deadlock_timeout = '200ms'")
        conn.exec_driver_sql("SET statement_timeout = '60s'")

        # 1) temp table (ใช้ TEXT ปลอดภัยกว่า ถ้า car_id เป็น uuid ก็ยังเก็บได้)
        conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_car_updates(
                quotation_num text PRIMARY KEY,
                car_id text NOT NULL
            ) ON COMMIT DROP
        """)

        # 2) bulk insert
        df_updates.to_sql(
            "tmp_car_updates",
            con=conn, if_exists="append", index=False,
            method="multi", chunksize=10_000
        )

        # 3) UPDATE จริง (อัปเดตเฉพาะที่เปลี่ยน/ยังว่าง)
        update_sql = text("""
            UPDATE fact_sales_quotation f
            SET car_id   = t.car_id,
                update_at = NOW()
            FROM tmp_car_updates t
            WHERE f.quotation_num = t.quotation_num
              AND (f.car_id IS NULL OR f.car_id IS DISTINCT FROM t.car_id)
        """)

        # 4) retry เมื่อเจอ deadlock
        max_retries = 5
        for attempt in range(max_retries):
            try:
                result = conn.execute(update_sql)
                print(f"🚀 Updated rows: {result.rowcount}")
                break
            except OperationalError as e:
                msg = str(getattr(e, "orig", e)).lower()
                if "deadlock detected" in msg and attempt < max_retries - 1:
                    sleep_s = (2 ** attempt) + random.random()
                    print(f"⚠️ Deadlock detected. Retrying in {sleep_s:.2f}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(sleep_s)
                    continue
                raise
        print("✅ Update car_id completed.")

# ---------- JOB ----------
@job
def update_fact_sales_quotation_car_id():
    update_car_id(
        filter_existing_fsq(
            join_with_dim_car(
                build_candidate_car_mapping(
                    extract_pay(),
                    extract_plan()
                ),
                extract_dim_car()
            ),
            extract_fsq_all_quotations()
        )
    )

# 👇 รันแบบสคริปต์เดี่ยว (ไม่ผ่าน Dagster UI) ก็ได้
# if __name__ == "__main__":
#     df_pay = extract_pay()
#     df_plan = extract_plan()
#     df_dim = extract_dim_car()
#     df_candidate = build_candidate_car_mapping(df_pay, df_plan)
#     df_joined = join_with_dim_car(df_candidate, df_dim)
#     df_fsq = extract_fsq_all_quotations()
#     df_updates = filter_existing_fsq(df_joined, df_fsq)
#     update_car_id(df_updates)
#     print("🎉 completed! car_id updated to fact_sales_quotation.")
