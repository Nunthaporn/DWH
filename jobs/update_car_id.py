from dagster import op, job
import os
import re
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =========================
# 🔧 ENV & DB CONNECTIONS
# =========================
load_dotenv()

# MariaDB (source)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True, pool_recycle=3600
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
    pool_pre_ping=True, pool_recycle=3600
)

# =========================
# 🔧 HELPERS
# =========================
NULL_TOKENS = {"", "nan", "none", "null", "undefined", "nat"}

def normalize_str(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
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
# 🧲 EXTRACT & CLEAN
# =========================
@op
def extract_and_clean_vin_keys() -> pd.DataFrame:
    with source_engine.begin() as conn:
        df_pay = pd.read_sql(
            text("""
                SELECT quo_num, TRIM(id_motor1) AS id_motor1, TRIM(id_motor2) AS id_motor2
                FROM fin_system_pay
            """),
            conn
        )
        df_plan = pd.read_sql(
            text("SELECT quo_num FROM fin_system_select_plan"),
            conn
        )

    df_pay = df_pay.drop_duplicates(subset=["quo_num"], keep="first")
    df_plan = df_plan.drop_duplicates(subset=["quo_num"], keep="first")

    df = pd.merge(df_pay, df_plan, on="quo_num", how="left")

    df = df.rename(columns={
        "quo_num": "quotation_num",
        "id_motor2": "car_vin",
        "id_motor1": "engine_number",
    })

    df["car_vin"] = normalize_str(df["car_vin"]).str.upper()
    df["engine_number"] = df["engine_number"].apply(clean_engine_number)

    # ตัด VIN ที่มีภาษาไทยหรือว่าง
    df = df[df["car_vin"].notna()]
    df = df[~df["car_vin"].apply(has_thai_chars)]

    # เลือก car_vin ล่าสุดถ้าซ้ำ
    df["_nonnull"] = df[["car_vin", "engine_number", "quotation_num"]].notna().sum(axis=1)
    df = df.sort_values("_nonnull", ascending=False).drop_duplicates(subset=["car_vin"], keep="first")
    df = df.drop(columns=["_nonnull"])

    df = df[["quotation_num", "car_vin", "engine_number"]].copy()
    print(f"✅ keys cleaned: {len(df):,} rows")
    return df


# =========================
# 🏭 LOAD dim_car
# =========================
@op
def fetch_dim_car_min() -> pd.DataFrame:
    with target_engine.begin() as conn:
        df_car = pd.read_sql(
            text("SELECT car_id, car_vin, engine_number FROM dim_car"),
            conn
        )
    df_car["car_vin"] = normalize_str(df_car["car_vin"]).str.upper()
    df_car["engine_number"] = df_car["engine_number"].apply(clean_engine_number)
    df_car = df_car.dropna(subset=["car_vin"])
    df_car = df_car[["car_id", "car_vin", "engine_number"]].drop_duplicates()
    print(f"✅ dim_car loaded: {len(df_car):,} rows")
    return df_car


# =========================
# 🔗 BUILD MAPPING (with fallback)
# =========================
@op
def build_car_mapping(df_keys: pd.DataFrame, df_dim: pd.DataFrame) -> pd.DataFrame:
    """
    1️⃣ join แบบ strict (VIN + ENGINE)
    2️⃣ ถ้าไม่เจอเลย fallback เป็น join VIN-only
    """
    df_strict = pd.merge(df_keys, df_dim, on=["car_vin", "engine_number"], how="inner")

    if df_strict.empty:
        print("⚠️ No strict matches (VIN+ENGINE). Trying VIN-only matching...")
        df_relaxed = pd.merge(df_keys, df_dim, on="car_vin", how="inner")
        print(f"✅ VIN-only matches: {len(df_relaxed):,} rows")

        if df_relaxed.empty:
            print("❌ No VIN-only matches found either.")
            return pd.DataFrame(columns=["quotation_num", "car_id"])

        df_relaxed = df_relaxed[["quotation_num", "car_id"]].drop_duplicates()
        return df_relaxed

    print(f"✅ mapping built (strict VIN+ENGINE): {len(df_strict):,} rows")
    df_strict = df_strict[["quotation_num", "car_id"]].drop_duplicates()
    return df_strict


# =========================
# 🚀 UPSERT INTO fact_sales_quotation
# =========================
@op
def upsert_car_ids(df_map: pd.DataFrame) -> int:
    if df_map.empty:
        print("⚠️ No mappings to upsert.")
        return 0

    with target_engine.begin() as conn:
        df_fact = pd.read_sql(text("SELECT quotation_num FROM fact_sales_quotation"), conn)

    need = (
        pd.merge(df_map, df_fact, on="quotation_num", how="inner")
        .drop_duplicates(subset=["quotation_num"])
        .copy()
    )

    if need.empty:
        print("⚠️ No rows matched existing facts.")
        return 0

    # 🧭 Debug ก่อน clean
    print(f"🔍 Raw before clean: {len(need):,} rows")
    print(need.head(5))
    print(need.dtypes)

    # 🧹 Clean
    need["quotation_num"] = need["quotation_num"].astype(str).str.strip()
    need["quotation_num"] = need["quotation_num"].replace(NULL_TOKENS, np.nan)

    # ✅ เปลี่ยนตรงนี้ให้รองรับ UUID string
    need["car_id"] = need["car_id"].astype(str).str.strip()
    need["car_id"] = need["car_id"].replace(NULL_TOKENS, np.nan)

    # 🧭 Debug หลัง clean
    print(f"🧾 Rows before dropna: {len(need):,}")
    print(need.isna().sum())

    need = need.dropna(subset=["quotation_num", "car_id"])
    print(f"✅ Rows after dropna: {len(need):,}")

    if need.empty:
        print("⚠️ No rows after cleaning.")
        return 0

    # ✅ Upsert
    with target_engine.begin() as conn:
        need.to_sql(
            "dim_car_temp",
            con=conn,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=100_000
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_dim_car_temp_quo ON dim_car_temp(quotation_num)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_dim_car_temp_carid ON dim_car_temp(car_id)"))
        print(f"✅ staged dim_car_temp: {len(need):,} rows")

        updated = conn.execute(text("""
            UPDATE fact_sales_quotation AS fsq
            SET car_id = t.car_id
            FROM dim_car_temp AS t
            WHERE fsq.quotation_num = t.quotation_num
              AND fsq.car_id IS DISTINCT FROM t.car_id;
        """)).rowcount or 0

        conn.execute(text("DROP TABLE IF EXISTS dim_car_temp"))
        print("🗑️ dropped dim_car_temp")

    print(f"✅ fact_sales_quotation updated: {updated} rows")
    return updated

# =========================
# 🧱 DAGSTER JOB
# =========================
@job
def update_car_id_on_fact():
    keys = extract_and_clean_vin_keys()
    dimc = fetch_dim_car_min()
    mapdf = build_car_mapping(keys, dimc)
    _ = upsert_car_ids(mapdf)


# =========================
# ▶️ LOCAL RUN
# =========================
if __name__ == "__main__":
    k = extract_and_clean_vin_keys()
    d = fetch_dim_car_min()
    m = build_car_mapping(k, d)
    updated = upsert_car_ids(m)
    print(f"🎉 done. updated rows = {updated}")


