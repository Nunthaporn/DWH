from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table, func
from psycopg2.extras import execute_batch
from datetime import datetime

load_dotenv()

# ============ DB CONNECTION ============
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True,
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
    pool_pre_ping=True,
)

# ============ EXTRACT ============
@op
def extract_agent_data():
    query = text("""
    SELECT 
        u.cuscode, u.name, u.rank, u.user_registered, u.status,
        u.fin_new_group, u.fin_new_mem, u.type_agent, u.typebuy,
        u.user_email, u.name_store, u.address, u.city, u.district,
        u.province, u.province_cur, u.area_cur, u.postcode, u.tel,
        u.date_active, u.display_name, u.headteam, u.status_vip,
        pr.career
    FROM wp_users u
    LEFT JOIN policy_register pr ON pr.cuscode = u.cuscode
    WHERE
        (u.cuscode = 'WEB-T2R')
        OR (
            u.user_login NOT IN ('FINTEST-01','FIN-TestApp','adminmag_fin','FNG00-00001')
            AND u.name NOT LIKE '%%test%%'
            AND u.name NOT LIKE '%%ทดสอบ%%'
        )
    """)
    df = pd.read_sql(query, source_engine)
    print(f"📦 Extracted rows: {len(df)}")
    return df

# ============ CLEAN ============
@op
def clean_agent_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["cuscode"] = df["cuscode"].astype(str).str.strip()

    # defect
    status_s = df["status"].astype(str).str.lower()
    df["defect_status"] = np.where(
        df["cuscode"].str.contains("-defect", case=False) | status_s.eq("defect"),
        "defect",
        None,
    )

    # join region
    df["agent_region"] = df["fin_new_group"].fillna('') + " " + df["fin_new_mem"].fillna('')
    df["agent_region"] = df["agent_region"].str.strip().replace('', np.nan)

    # rename
    rename_map = {
        "cuscode": "agent_id",
        "name": "agent_name",
        "rank": "agent_rank",
        "user_registered": "hire_date",
        "type_agent": "type_agent",
        "typebuy": "is_experienced",
        "user_email": "agent_email",
        "name_store": "store_name",
        "address": "agent_address",
        "city": "subdistrict",
        "district": "district",
        "province": "province",
        "province_cur": "current_province",
        "area_cur": "current_area",
        "postcode": "zipcode",
        "tel": "mobile_number",
        "career": "job",
    }
    df.rename(columns=rename_map, inplace=True)

    # clean basic strings
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip().replace({"": None, "nan": None, "NaN": None})

    # clean email (vectorized)
    mask_valid = df["agent_email"].astype(str).str.match(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
    mask_thai = df["agent_email"].astype(str).str.contains(r"[ก-๙]")
    df.loc[~mask_valid | mask_thai, "agent_email"] = None

    # mobile digits only
    df["mobile_number"] = df["mobile_number"].astype(str).str.replace(r"\D", "", regex=True)

    # hire_date → int
    dt = pd.to_datetime(df["hire_date"], errors="coerce")
    df["hire_date"] = dt.dt.strftime("%Y%m%d").astype("Int64")

    # date_active → datetime
    df["date_active"] = pd.to_datetime(df["date_active"], errors="coerce")

    print(f"✅ Cleaned rows: {len(df)}")
    return df

# ============ LOAD (BULK UPSERT) ============
@op
def load_to_wh(df: pd.DataFrame):
    table_name = "dim_agent"
    pk = "agent_id"

    df = df.where(pd.notnull(df), None)
    cols = list(df.columns)

    insert_sql = f"""
    INSERT INTO {table_name} ({', '.join(cols)})
    VALUES ({', '.join(['%s'] * len(cols))})
    ON CONFLICT ({pk}) DO UPDATE
    SET {', '.join([f"{c} = EXCLUDED.{c}" for c in cols if c not in [pk, 'create_at']])},
        update_at = NOW()
    """

    records = [tuple(x) for x in df[cols].to_numpy()]
    print(f"🚀 Upserting {len(records)} records...")

    with target_engine.raw_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, records, page_size=5000)
        conn.commit()

    print("✅ Upsert completed via execute_batch")

# ============ BACKFILL DATE_ACTIVE ============
@op
def backfill_date_active(df: pd.DataFrame):
    df = df[[ "agent_id", "date_active" ]].dropna(subset=["agent_id"])
    df["date_active"] = pd.to_datetime(df["date_active"], errors="coerce")

    records = [
        (r.agent_id, r.date_active) for r in df.itertuples(index=False)
        if pd.notna(r.date_active)
    ]
    if not records:
        print("⚠️ No valid date_active found — skip")
        return

    sql = """
    UPDATE dim_agent AS t
    SET date_active = s.date_active,
        update_at = NOW()
    FROM (VALUES %s) AS s(agent_id, date_active)
    WHERE t.agent_id = s.agent_id
      AND t.date_active IS DISTINCT FROM s.date_active;
    """

    with target_engine.raw_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, records, page_size=5000)
        conn.commit()

    print(f"✅ Backfilled {len(records)} rows of date_active")

# ============ JOB ============
@job
def dim_agent_etl():
    df = extract_agent_data()
    df_clean = clean_agent_data(df)
    load_to_wh(df_clean)
    backfill_date_active(df_clean)

if __name__ == "__main__":
    df = extract_agent_data()
    df_clean = clean_agent_data(df)
    load_to_wh(df_clean)
    backfill_date_active(df_clean)
    print("🎉 Completed fast ETL")


