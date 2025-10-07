from dagster import op, job
import os, re
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_batch
import io

# =========================
# 🔧 ENV & DB CONNECTIONS
# =========================
load_dotenv()

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
    query = text("""
        SELECT 
            fsp.quo_num, fsp.type_insure, fsp.type_work, fsp.type_status,
            fsp.type_key, fsp.app_type, fsp.chanel_key, fsp.token,
            fsp.in_advance, fsp.check_tax,
            fo.worksend
        FROM fin_system_select_plan fsp
        LEFT JOIN fininsurance_task.fin_order fo ON fsp.quo_num = fo.quo_num
    """)
    df = pd.read_sql(query, source_engine_main)
    print(f"📦 Extracted rows: {len(df):,}")
    return df

# =========================
# ⚙️ TRANSFORM (vectorized)
# =========================
@op
def transform_build_keys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["key_channel"] = np.select(
        [
            df["chanel_key"].str.contains("app", case=False, na=False),
            df["chanel_key"].str.contains("web", case=False, na=False)
        ],
        ["APP", "WEB"],
        default=None
    )

    # Fallback rules
    df.loc[df["key_channel"].isna() & df["token"].notna(), "key_channel"] = "WEB"
    df.loc[df["key_channel"].isna() & df["token"].isna(), "key_channel"] = "APP"

    # Rename columns
    df.rename(columns={
        "quo_num": "quotation_num",
        "type_insure": "type_insurance",
        "type_work": "order_type",
        "type_status": "check_type",
        "worksend": "work_type"
    }, inplace=True)

    # Rule: order_type 1 → NULL
    with np.errstate(all='ignore'):
        df["order_type"] = pd.to_numeric(df["order_type"], errors="coerce")
    df.loc[df["order_type"] == 1, "order_type"] = np.nan

    # Override order_type
    df["order_type"] = np.select(
        [df["in_advance"] == 1, df["check_tax"] == 1],
        ["งานต่ออายุล่วงหน้า", "งานต่อภาษี"],
        default=df["order_type"]
    )

    cols = ["quotation_num", "type_insurance", "order_type", "check_type", "work_type", "key_channel"]
    df = df[cols].drop_duplicates(subset=["quotation_num"], keep="first")
    df = df.where(pd.notnull(df), None)
    print(f"🧩 Transformed rows: {len(df):,}")
    return df

# =========================
# 📖 LOAD DIM
# =========================
@op
def fetch_dim_order_type() -> pd.DataFrame:
    with target_engine.begin() as conn:
        df = pd.read_sql(
            text("SELECT order_type_id, type_insurance, order_type, check_type, work_type, key_channel FROM dim_order_type"),
            conn
        )
    print(f"📗 Loaded dim_order_type: {len(df):,} rows")
    return df

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
    out = df[["quotation_num", "order_type_id"]].dropna(subset=["order_type_id"])
    print(f"🔗 Mapped quotations → order_type_id: {len(out):,}")
    return out

# =========================
# 🚀 UPSERT (optimized COPY)
# =========================
@op
def upsert_order_type_ids(df_pairs: pd.DataFrame) -> int:
    if df_pairs.empty:
        print("⚠️ No rows to update.")
        return 0

    records = [tuple(x) for x in df_pairs.to_numpy()]
    print(f"🚀 Updating {len(records):,} quotations in fact_sales_quotation")

    with target_engine.raw_connection() as conn:
        cur = conn.cursor()

        # ใช้ temp table แบบ ON COMMIT DROP (ไม่ต้อง drop เอง)
        cur.execute("""
            CREATE TEMP TABLE tmp_order_type (
                quotation_num VARCHAR PRIMARY KEY,
                order_type_id INT
            ) ON COMMIT DROP;
        """)

        # COPY FROM memory (เร็วกว่า to_sql 20 เท่า)
        buf = io.StringIO()
        df_pairs.to_csv(buf, sep="\t", header=False, index=False)
        buf.seek(0)
        cur.copy_from(buf, "tmp_order_type", null="")

        # Update ทีเดียว
        cur.execute("""
            UPDATE fact_sales_quotation fsq
            SET order_type_id = tmp.order_type_id
            FROM tmp_order_type tmp
            WHERE fsq.quotation_num = tmp.quotation_num
              AND fsq.order_type_id IS DISTINCT FROM tmp.order_type_id;
        """)

        updated = cur.rowcount or 0
        conn.commit()

    print(f"✅ Updated fact_sales_quotation: {updated:,} rows")
    return updated

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
# ▶️ LOCAL RUN
# =========================
if __name__ == "__main__":
    m = extract_merge_sources()
    k = transform_build_keys(m)
    d = fetch_dim_order_type()
    p = join_to_dim_order_type(k, d)
    updated = upsert_order_type_ids(p)
    print(f"🎉 done. updated rows = {updated}")
