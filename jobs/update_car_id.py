from dagster import op, job
import os
import re
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

# =========================
# 🔧 ENV & DB CONNECTIONS
# =========================
load_dotenv()

# MariaDB (source)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True,
    pool_recycle=3600,
)

# PostgreSQL (target)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@"
    f"{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c statement_timeout=300000",
    },
    pool_pre_ping=True,
    pool_recycle=3600,
)

# =========================
# 🔧 HELPERS
# =========================
NULL_TOKENS = {"", "nan", "none", "null", "undefined", "nat"}

def normalize_str(s: pd.Series) -> pd.Series:
    s = s.astype("string")
    s = s.str.strip()
    s = s.mask(s.str.len() == 0)
    s = s.mask(s.str.lower().isin(NULL_TOKENS))
    return s

def clean_engine_number(v):
    if pd.isna(v):
        return None
    cleaned = re.sub(r"[^A-Za-z0-9]", "", str(v))
    return cleaned.upper() if cleaned else None

def has_thai_chars(v):
    if pd.isna(v):
        return False
    return bool(re.search(r"[ก-๙]", str(v)))

# =========================
# 🧲 EXTRACT & CLEAN (จาก MariaDB)
# =========================
@op
def extract_and_clean_vin_keys() -> pd.DataFrame:
    """
    ดึง quo_num, id_motor1(เครื่อง), id_motor2(VIN) แล้วทำความสะอาด
    คืน DataFrame: [quotation_num, car_vin, engine_number]
    """
    # pay
    df_pay = pd.read_sql(
        """
        SELECT quo_num, TRIM(id_motor1) AS id_motor1, TRIM(id_motor2) AS id_motor2
        FROM fin_system_pay
        """,
        source_engine,
    )

    # plan (มีไว้เพื่อ ensure ว่า quo_num นี้เคยอยู่ในระบบจริง)
    df_plan = pd.read_sql(
        """
        SELECT quo_num
        FROM fin_system_select_plan
        """,
        source_engine,
    )

    # dedup ด้วย quo_num ก่อน
    df_pay = df_pay.drop_duplicates(subset=["quo_num"], keep="first")
    df_plan = df_plan.drop_duplicates(subset=["quo_num"], keep="first")

    # merge
    df = pd.merge(df_pay, df_plan, on="quo_num", how="left")

    # rename
    df = df.rename(
        columns={"quo_num": "quotation_num", "id_motor2": "car_vin", "id_motor1": "engine_number"}
    )

    # ensure columns exist
    if "car_vin" not in df.columns:
        df["car_vin"] = None
    if "engine_number" not in df.columns:
        df["engine_number"] = None

    # normalize
    df["car_vin"] = normalize_str(df["car_vin"]).str.upper()
    df["engine_number"] = df["engine_number"].apply(clean_engine_number)

    # กรอง VIN ที่เป็นค่าว่าง/มีอักษรไทย
    df = df[df["car_vin"].notna()]
    df = df[~df["car_vin"].apply(has_thai_chars)]

    # เก็บ record ที่ field ไม่ว่างเยอะกว่าเมื่อซ้ำ VIN
    df["_nonnull"] = df[["car_vin", "engine_number", "quotation_num"]].notna().sum(axis=1)
    df = df.sort_values("_nonnull", ascending=False).drop_duplicates(subset=["car_vin"], keep="first")
    df = df.drop(columns=["_nonnull"])

    # จบด้วย 3 คอลัมน์ที่ต้องใช้
    df = df[["quotation_num", "car_vin", "engine_number"]].copy()
    print(f"✅ keys cleaned: {len(df):,} rows")
    return df

# =========================
# 🏭 LOAD dim_car (จาก PostgreSQL)
# =========================
@op
def fetch_dim_car_min() -> pd.DataFrame:
    df_car = pd.read_sql(text("SELECT car_id, car_vin, engine_number FROM dim_car"), target_engine)
    # ทำความสะอาดให้เข้ากัน
    df_car["car_vin"] = normalize_str(df_car["car_vin"]).str.upper()
    df_car["engine_number"] = df_car["engine_number"].apply(clean_engine_number)
    df_car = df_car.dropna(subset=["car_vin"])  # ต้องมี VIN
    print(f"✅ dim_car loaded: {len(df_car):,} rows")
    return df_car[["car_id", "car_vin", "engine_number"]].drop_duplicates()

# =========================
# 🔗 BUILD MAPPING (VIN+ENGINE -> car_id)
# =========================
@op
def build_car_mapping(df_keys: pd.DataFrame, df_dim: pd.DataFrame) -> pd.DataFrame:
    """
    join แบบ strict ด้วย (car_vin, engine_number)
    คืน DataFrame: [quotation_num, car_id]
    """
    # inner join
    df_map = pd.merge(
        df_keys, df_dim, on=["car_vin", "engine_number"], how="inner"
    )  # ได้ [quotation_num, car_vin, engine_number, car_id]

    # เลือกเฉพาะ 2 คอลัมน์ที่ต้องใช้ และกันซ้ำ quotation_num
    df_map = df_map[["quotation_num", "car_id"]].copy()
    df_map = df_map.drop_duplicates(subset=["quotation_num"], keep="first")
    print(f"✅ mapping built (strict VIN+ENGINE): {len(df_map):,} rows")
    return df_map

# =========================
# 🧹 FILTER เฉพาะที่มีอยู่จริงใน fact
# =========================
@op
def restrict_to_existing_quotes(df_map: pd.DataFrame) -> pd.DataFrame:
    df_fact = pd.read_sql(text("SELECT quotation_num FROM fact_sales_quotation"), target_engine)
    out = pd.merge(df_map, df_fact, on="quotation_num", how="inner")
    out = out.drop_duplicates(subset=["quotation_num"])
    print(f"✅ mapping filtered by existing facts: {len(out):,} rows")
    return out[["quotation_num", "car_id"]]

# =========================
# 🧼 STAGE TEMP TABLE
# =========================
@op
def stage_dim_car_temp(df_map: pd.DataFrame) -> str:
    """
    สร้างตารางชั่วคราว dim_car_temp (quotation_num, car_id)
    """
    if df_map.empty:
        # ให้มี schema ถูกต้องแม้ไม่มีข้อมูล
        df_map = pd.DataFrame(
            {"quotation_num": pd.Series(dtype="string"), "car_id": pd.Series(dtype="Int64")}
        )

    # sanitize
    df_map = df_map.copy()
    df_map["quotation_num"] = normalize_str(df_map["quotation_num"])
    # car_id อาจเป็น int/float; แคสต์เป็น Int64 (nullable) แล้วค่อย to_sql
    df_map["car_id"] = pd.to_numeric(df_map["car_id"], errors="coerce").astype("Int64")

    df_map.to_sql("dim_car_temp", target_engine, if_exists="replace", index=False, method="multi", chunksize=20000)
    print(f"✅ staged dim_car_temp: {len(df_map):,} rows")
    return "dim_car_temp"

# =========================
# 🚀 APPLY UPDATE TO FACT
# =========================
@op
def update_fact_car_id(temp_table_name: str) -> int:
    """
    อัปเดต fact_sales_quotation.car_id จาก temp
    อัปเดตเฉพาะแถวที่ค่า 'ต่างจริง' (NULL-safe)
    """
    if not temp_table_name:
        print("⚠️ temp table name missing; skip update.")
        return 0

    update_sql = text(f"""
        UPDATE fact_sales_quotation fsq
        SET car_id = dc.car_id
        FROM {temp_table_name} dc
        WHERE fsq.quotation_num = dc.quotation_num;
    """)

    with target_engine.begin() as conn:
        res = conn.execute(update_sql)
        print(f"✅ fact_sales_quotation updated: {res.rowcount} rows")
        return res.rowcount or 0

# =========================
# 🧹 DROP TEMP
# =========================
@op
def drop_dim_car_temp(temp_table_name: str) -> None:
    if not temp_table_name:
        return
    with target_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))
    print("🗑️ dropped dim_car_temp")

# =========================
# 🧱 DAGSTER JOB
# =========================
@job
def update_car_id_on_fact():
    keys = extract_and_clean_vin_keys()
    dimc = fetch_dim_car_min()
    mapping = build_car_mapping(keys, dimc)
    mapping_in_fact = restrict_to_existing_quotes(mapping)
    temp = stage_dim_car_temp(mapping_in_fact)
    _ = update_fact_car_id(temp)
    drop_dim_car_temp(temp)

# =========================
# ▶️ LOCAL RUN (optional)
# # =========================
# if __name__ == "__main__":
#     k = extract_and_clean_vin_keys()
#     d = fetch_dim_car_min()
#     m = build_car_mapping(k, d)
#     mf = restrict_to_existing_quotes(m)
#     t = stage_dim_car_temp(mf)
#     updated = update_fact_car_id(t)
#     drop_dim_car_temp(t)
#     print(f"🎉 done. updated rows = {updated}")
