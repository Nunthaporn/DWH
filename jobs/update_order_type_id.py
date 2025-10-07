from dagster import op, job
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =========================
# 🔧 ENV & DB CONNECTIONS
# =========================
load_dotenv()

# MariaDB (source)
source_engine_main = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True, pool_recycle=3600
)
source_engine_task = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
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
# 🧲 EXTRACT
# =========================
@op
def extract_merge_sources() -> pd.DataFrame:
    with source_engine_main.begin() as conn_main, source_engine_task.begin() as conn_task:
        df = pd.read_sql(text("""
            SELECT quo_num, type_insure, type_work, type_status, type_key, 
                   app_type, chanel_key, token, in_advance, check_tax
            FROM fin_system_select_plan
            WHERE quo_num >= 'FQ2400'  -- ✅ filter ให้เร็วขึ้น (ปรับได้ตามช่วง)
        """), conn_main)
        df1 = pd.read_sql(text("""
            SELECT quo_num, worksend
            FROM fin_order
        """), conn_task)
    return pd.merge(df, df1, on="quo_num", how="left")

# =========================
# 🧼 TRANSFORM (Vectorized)
# =========================
@op
def transform_build_keys(df_merged: pd.DataFrame) -> pd.DataFrame:
    df = df_merged.copy()

    # normalize lowercase
    df["type_key_norm"] = df["type_key"].fillna("").str.lower()

    # ✅ vectorized base extraction
    df["base"] = np.select(
        [
            df["type_key_norm"].str.contains(r"app|application|mobile|แอป", regex=True, na=False),
            df["type_key_norm"].str.contains(r"web|website|เวบ|เว็บไซต์", regex=True, na=False),
        ],
        ["APP", "WEB"],
        default=None
    )

    # ✅ vectorized subtype extraction
    df["subtype"] = (
        df["chanel_key"].fillna("").str.upper()
        .str.extract(r"(B2B|B2C|TELE|THAIPOST|THAICARE)", expand=False)
    )

    # combine base + subtype
    df["key_channel"] = np.where(
        df["subtype"].notna(),
        (df["base"].fillna("") + " " + df["subtype"].fillna("")).str.strip(),
        df["base"]
    )

    # ✅ fallback จาก token
    df["key_channel"] = np.where(
        df["key_channel"].isna() | (df["key_channel"].astype(str).str.strip() == ""),
        np.where(
            df["token"].notna() & (df["token"].astype(str).str.strip() != ""), "WEB", "APP"
        ),
        df["key_channel"]
    )

    # ✅ override order_type ด้วย in_advance/check_tax
    df["order_type"] = np.select(
        [
            df["in_advance"] == 1,
            df["check_tax"] == 1
        ],
        [
            "งานต่ออายุล่วงหน้า",
            "งานต่อภาษี"
        ],
        default=df["type_work"]
    )

    # ✅ rename columns
    df.rename(columns={
        "quo_num": "quotation_num",
        "type_insure": "type_insurance",
        "type_status": "check_type",
        "worksend": "work_type",
    }, inplace=True)

    # ✅ clean empty text
    df = df.replace(r"^\s*$", np.nan, regex=True)
    df = df.drop_duplicates(subset=["quotation_num"], keep="first")

    return df[["quotation_num", "type_insurance", "order_type", "check_type", "work_type", "key_channel"]]

# =========================
# 📖 Load dim_order_type
# =========================
@op
def fetch_dim_order_type() -> pd.DataFrame:
    with target_engine.begin() as conn:
        return pd.read_sql(
            text("SELECT order_type_id, type_insurance, order_type, check_type, work_type, key_channel FROM dim_order_type"),
            conn
        )

# =========================
# 🔗 JOIN
# =========================
@op
def join_to_dim_order_type(df_keys: pd.DataFrame, df_dim: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(
        df_keys, df_dim,
        on=["type_insurance", "order_type", "check_type", "work_type", "key_channel"],
        how="left"
    )
    return df[["quotation_num", "order_type_id"]].dropna(subset=["order_type_id"])

# =========================
# 🚀 UPSERT (optimized)
# =========================
@op
def upsert_order_type_ids(df_pairs: pd.DataFrame) -> int:
    if df_pairs.empty:
        print("⚠️ No rows to update.")
        return 0

    with target_engine.connect() as conn:
        with conn.begin():
            df_pairs.to_sql(
                "dim_order_type_temp",
                con=conn,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=100_000
            )
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_dim_order_type_temp_q ON dim_order_type_temp(quotation_num)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_sales_quotation_q ON fact_sales_quotation(quotation_num)"))

            print(f"✅ staged dim_order_type_temp: {len(df_pairs):,} rows")

            # ✅ batch update (ลด lock time)
            quotation_list = df_pairs["quotation_num"].tolist()
            chunk_size = 50000
            updated_total = 0

            for i in range(0, len(quotation_list), chunk_size):
                sub = quotation_list[i:i + chunk_size]
                updated = conn.execute(text("""
                    UPDATE fact_sales_quotation AS fsq
                    SET order_type_id = dc.order_type_id
                    FROM dim_order_type_temp AS dc
                    WHERE fsq.quotation_num = dc.quotation_num
                      AND fsq.order_type_id IS DISTINCT FROM dc.order_type_id
                      AND fsq.quotation_num = ANY(:sublist);
                """), {"sublist": sub}).rowcount or 0
                updated_total += updated
                print(f"  🔸 batch {i//chunk_size+1}: {updated:,} rows")

            conn.execute(text("DROP TABLE IF EXISTS dim_order_type_temp"))
            print("🗑️ dropped dim_order_type_temp")

    print(f"✅ fact_sales_quotation updated: {updated_total:,} rows")
    return updated_total

# =========================
# 🧱 DAGSTER JOB
# =========================
@job
def update_order_type_id_on_fact():
    merged = extract_merge_sources()
    keys   = transform_build_keys(merged)
    dim    = fetch_dim_order_type()
    pairs  = join_to_dim_order_type(keys, dim)
    _      = upsert_order_type_ids(pairs)

# =========================
# ▶️ Local run (optional)
# =========================
if __name__ == "__main__":
    merged = extract_merge_sources()
    keys   = transform_build_keys(merged)
    dim    = fetch_dim_order_type()
    pairs  = join_to_dim_order_type(keys, dim)
    updated = upsert_order_type_ids(pairs)
    print(f"🎉 done. updated rows = {updated:,}")
