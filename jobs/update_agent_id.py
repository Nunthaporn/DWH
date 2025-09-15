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
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")

# MariaDB (source)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True, pool_recycle=3600
)

# PostgreSQL (target) — set search_path + timeout
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@"
    f"{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    connect_args={
        "keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 5,
        "options": f"-c search_path={PG_SCHEMA} -c statement_timeout=300000"
    },
    pool_pre_ping=True, pool_recycle=3600
)

# =========================
# 🔧 HELPERS
# =========================
NULL_TOKENS = {"", "nan", "none", "null", "undefined"}

def normalize_str_col(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    s = s.mask(s.str.len() == 0)
    s = s.mask(s.str.lower().isin(NULL_TOKENS))
    return s

def base_id(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.replace(r"-defect$", "", regex=True)

# =========================
# 🧲 EXTRACT + TRANSFORM
# =========================
@op
def extract_agent_mapping() -> pd.DataFrame:
    """สร้าง mapping [quotation_num, agent_id] จาก MySQL + Postgres โดย normalize ตาม dim_agent"""
    # source: fin_system_pay
    with source_engine.begin() as sconn:
        df_career = pd.read_sql(text("SELECT quo_num, id_cus FROM fin_system_pay"), sconn)
    df_career = df_career.rename(columns={"id_cus": "agent_id", "quo_num": "quotation_num"})
    df_career["agent_id"] = normalize_str_col(df_career["agent_id"])

    # fact quotations (ทั้งหมด)
    with target_engine.begin() as tconn:
        df_fact = pd.read_sql(text(f"SELECT quotation_num FROM {PG_SCHEMA}.fact_sales_quotation"), tconn)

    # คง quotation ทั้งหมด
    df_m1 = pd.merge(df_career, df_fact, on="quotation_num", how="right")

    # dim_agent เพื่อทำ standardization (prefer defect ถ้ามีทั้งคู่)
    with target_engine.begin() as tconn:
        df_main = pd.read_sql(text(f"SELECT agent_id FROM {PG_SCHEMA}.dim_agent"), tconn)
    df_main["agent_id"] = normalize_str_col(df_main["agent_id"]).dropna()

    dfm = df_main.copy()
    dfm["__base"] = base_id(dfm["agent_id"])
    dfm["__is_defect"] = dfm["agent_id"].str.contains(r"-defect$", case=False, na=False)

    dup_mask = dfm["__base"].duplicated(keep=False)
    main_single = dfm[~dup_mask].copy()
    main_dups = (
        dfm[dup_mask]
        .sort_values(["__base", "__is_defect"])
        .drop_duplicates("__base", keep="last")
    )
    df_main_norm = pd.concat([main_single, main_dups], ignore_index=True)

    # join ด้วย base_id
    df_m1["__base"] = base_id(df_m1["agent_id"])
    df_main_norm["__base"] = base_id(df_main_norm["agent_id"])

    df_join = pd.merge(
        df_m1,
        df_main_norm.drop(columns=["__is_defect"], errors="ignore"),
        on="__base", how="left", suffixes=("_m1", "_main")
    )

    # เลือก agent_id_final (main ถ้ามี ไม่งั้นใช้ของเดิม)
    if "agent_id_m1" not in df_join.columns:  df_join["agent_id_m1"] = pd.NA
    if "agent_id_main" not in df_join.columns: df_join["agent_id_main"] = pd.NA

    df_join["agent_id_final"] = np.where(
        df_join["agent_id_main"].notna(), df_join["agent_id_main"], df_join["agent_id_m1"]
    )

    df_out = df_join[["quotation_num", "agent_id_final"]].rename(columns={"agent_id_final": "agent_id"})
    df_out["agent_id"] = normalize_str_col(df_out["agent_id"])
    df_out["__has_agent"] = df_out["agent_id"].notna().astype(int)
    df_out = (
        df_out.sort_values(["quotation_num", "__has_agent"], ascending=[True, False])
              .drop_duplicates("quotation_num", keep="first")
              .drop(columns="__has_agent")
    )

    # กันค่าว่าง key
    df_out = df_out.dropna(subset=["quotation_num"])
    print(f"✅ extract_agent_mapping → {len(df_out):,} rows")
    return df_out

# =========================
# 🧼 STAGE TEMP (DDL เสมอ)
# =========================
@op
def stage_dim_agent_temp(df_map: pd.DataFrame) -> str:
    """
    สร้าง {schema}.dim_agent_temp ด้วย DDL เสมอ แล้ว append ข้อมูล (ถ้ามี)
    และ normalize agent_id casing ให้ตรงกับ dim_agent
    """
    tbl = "dim_agent_temp"
    full_tbl = f"{PG_SCHEMA}.{tbl}"

    # เตรียมข้อมูล
    if df_map is None:
        df_map = pd.DataFrame()
    tmp = (df_map.replace(["None","none","nan","NaN","NaT",""], pd.NA)).copy()
    tmp["quotation_num"] = normalize_str_col(tmp.get("quotation_num", pd.Series(dtype="string")))
    tmp["agent_id"]      = normalize_str_col(tmp.get("agent_id",      pd.Series(dtype="string")))

    # DDL: create table เสมอ + index
    with target_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {full_tbl};"))
        conn.execute(text(f"""
            CREATE TABLE {full_tbl} (
                quotation_num text,
                agent_id      text
            );
        """))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_dim_agent_temp_quo   ON {full_tbl} (quotation_num);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_dim_agent_temp_agent ON {full_tbl} (LOWER(agent_id));"))

    # append เฉพาะแถวที่มี key
    to_append = tmp.dropna(subset=["quotation_num"])
    if not to_append.empty:
        with target_engine.begin() as conn:
            to_append.to_sql("dim_agent_temp", con=conn, schema=PG_SCHEMA,
                             if_exists="append", index=False, method="multi", chunksize=20000)
        print(f"✅ staged → {len(to_append):,} rows into {full_tbl}")
    else:
        print(f"⚠️ no rows to stage, created empty table → {full_tbl}")

    # normalize casing ให้ตรง dim_agent
    with target_engine.begin() as conn:
        res = conn.execute(text(f"""
            UPDATE {full_tbl} t
            SET agent_id = da.agent_id
            FROM {PG_SCHEMA}.dim_agent da
            WHERE LOWER(da.agent_id) = LOWER(t.agent_id)
              AND t.agent_id IS DISTINCT FROM da.agent_id;
        """))
        print(f"🔄 normalized agent_id casing: {res.rowcount} rows")

    return full_tbl  # ส่งคืนชื่อ fully-qualified

# =========================
# 🚀 UPDATE FACT (เช็คตารางก่อน)
# =========================
@op
def update_fact_from_temp(temp_table_name: str) -> int:
    if not temp_table_name:
        print("⚠️ temp table name missing, skip update.")
        return 0

    # ตรวจว่ามีตารางจริง (กันกรณีผิด schema/name)
    with target_engine.begin() as conn:
        exists = conn.execute(text("SELECT to_regclass(:fqname)"), {"fqname": temp_table_name}).scalar()
    if not exists:
        print(f"❌ temp not found: {temp_table_name}")
        return 0

    with target_engine.begin() as conn:
        res = conn.execute(text(f"""
            UPDATE {PG_SCHEMA}.fact_sales_quotation fsq
            SET agent_id = da.agent_id
            FROM {temp_table_name} dc
            JOIN {PG_SCHEMA}.dim_agent da
              ON LOWER(da.agent_id) = LOWER(dc.agent_id)
            WHERE fsq.quotation_num = dc.quotation_num
              AND fsq.agent_id IS DISTINCT FROM da.agent_id;
        """))
        print(f"✅ updated fact rows: {res.rowcount}")
        return res.rowcount or 0

# =========================
# 🗑️ DROP TEMP (บังคับให้รอ update ด้วยการรับ updated_count)
# =========================
@op
def drop_dim_agent_temp(temp_table_name: str, updated_count: int) -> None:  # noqa: ARG002 (unused)
    if not temp_table_name:
        return
    with target_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
    print(f"🗑️ dropped {temp_table_name}")

# =========================
# 🧱 DAGSTER JOB
# =========================
@job
def update_agent_id_on_fact():
    df = extract_agent_mapping()
    temp_full = stage_dim_agent_temp(df)
    updated = update_fact_from_temp(temp_full)      # <- ต้องเสร็จก่อน
    drop_dim_agent_temp(temp_full, updated)         # <- แล้วค่อย drop (dependency ผูกด้วย updated)
