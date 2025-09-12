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
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": f"-c search_path={PG_SCHEMA} -c statement_timeout=300000",
    },
    pool_pre_ping=True, pool_recycle=3600
)

# =========================
# 🔧 HELPERS
# =========================
def base_id_series(s: pd.Series) -> pd.Series:
    """คืนค่า agent_id ที่ตัด suffix '-defect' ออกสำหรับทำคีย์กลาง"""
    return s.astype(str).str.strip().str.replace(r"-defect$", "", regex=True)

def normalize_str_col(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    s = s.mask(s.str.len() == 0)
    s = s.mask(s.str.lower().isin(["nan", "none", "null", "undefined"]))
    return s

# =========================
# 🧲 EXTRACT + TRANSFORM
# =========================
@op
def extract_agent_mapping() -> pd.DataFrame:
    """
    ดึง agent จาก fin_system_pay (source) + quotation จาก fact_sales_quotation (target)
    ทำ standardization agent_id ด้วยฐาน dim_agent
    """
    # 1) source: fin_system_pay
    with source_engine.begin() as sconn:
        df_career = pd.read_sql(
            text("SELECT quo_num, id_cus FROM fin_system_pay"),
            sconn
        )
    df_career = df_career.rename(columns={"id_cus": "agent_id", "quo_num": "quotation_num"})
    df_career["agent_id"] = normalize_str_col(df_career["agent_id"])

    # 2) fact quotations
    with target_engine.begin() as tconn:
        df_fact = pd.read_sql(text(f"SELECT quotation_num FROM {PG_SCHEMA}.fact_sales_quotation"), tconn)

    # 3) right-join (ให้คง quotation ทั้งหมด)
    df_m1 = pd.merge(df_career, df_fact, on="quotation_num", how="right")

    # 4) dim_agent สำหรับสร้าง standard casing/ตัวแทน
    with target_engine.begin() as tconn:
        df_main = pd.read_sql(text(f"SELECT agent_id FROM {PG_SCHEMA}.dim_agent"), tconn)
    df_main["agent_id"] = normalize_str_col(df_main["agent_id"]).dropna()

    # 4.1) ทำคีย์ base และเลือกตัวแทน/ตัว defect
    dfm = df_main.copy()
    dfm["__base"] = base_id_series(dfm["agent_id"])
    dfm["__is_defect"] = dfm["agent_id"].str.contains(r"-defect$", case=False, na=False)
    dup_mask = dfm["__base"].duplicated(keep=False)

    main_single = dfm[~dup_mask].copy()
    main_dups = (
        dfm[dup_mask]
        .sort_values(["__base", "__is_defect"])
        .drop_duplicates("__base", keep="last")  # defect จะถูกเลือกถ้ามี
    )
    df_main_norm = pd.concat([main_single, main_dups], ignore_index=True)

    # 5) ทำคีย์กลางที่ mapping
    df_m1["__base"] = base_id_series(df_m1["agent_id"])

    # 6) จับคู่ด้วย base
    df_join = pd.merge(
        df_m1,
        df_main_norm.drop(columns=["__is_defect"], errors="ignore"),
        on="__base",
        how="left",
        suffixes=("_m1", "_main")
    )

    # 7) agent_id_final: ถ้าแมตช์มิติได้ใช้ของมิติ, ไม่งั้นใช้ของเดิม
    df_join["agent_id_final"] = np.where(
        df_join["agent_id"].notna() & df_join["agent_id_main"].notna(),
        df_join["agent_id_main"],
        df_join["agent_id_m1"]
    )

    # 8) คืนผลและกันซ้ำ
    df_out = df_join[["quotation_num", "agent_id_final"]].rename(columns={"agent_id_final": "agent_id"})
    df_out["agent_id"] = normalize_str_col(df_out["agent_id"])
    df_out["__has_agent"] = df_out["agent_id"].notna().astype(int)
    df_out = (
        df_out.sort_values(["quotation_num", "__has_agent"], ascending=[True, False])
              .drop_duplicates("quotation_num", keep="first")
              .drop(columns="__has_agent")
    )
    return df_out

# =========================
# 🧹 STAGE TEMP TABLE
# =========================
@op
def stage_dim_agent_temp(df_map: pd.DataFrame) -> str:
    """
    สร้างตารางชั่วคราว <schema>.dim_agent_temp (quotation_num, agent_id)
    และ normalize casing ให้ตรง dim_agent
    """
    tbl = "dim_agent_temp"
    full_tbl = f"{PG_SCHEMA}.{tbl}"

    if df_map.empty:
        df_map = pd.DataFrame({
            "quotation_num": pd.Series(dtype="string"),
            "agent_id": pd.Series(dtype="string")
        })

    # sanitize
    df_tmp = df_map.copy()
    df_tmp["quotation_num"] = normalize_str_col(df_tmp["quotation_num"])
    df_tmp["agent_id"] = normalize_str_col(df_tmp["agent_id"])
    df_tmp = df_tmp.dropna(subset=["quotation_num"])

    with target_engine.begin() as conn:
        df_tmp.to_sql(
            tbl, con=conn, schema=PG_SCHEMA,
            if_exists="replace", index=False, method="multi", chunksize=20_000
        )
        # index ช่วยเร่งความเร็ว
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{tbl}_quo ON {full_tbl} (quotation_num)'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{tbl}_agent ON {full_tbl} (LOWER(agent_id))'))
    print(f"✅ staged to {full_tbl}: {len(df_tmp):,} rows")

    # Normalize casing ให้ตรง dim_agent
    normalize_query = text(f"""
        UPDATE {full_tbl} t
        SET agent_id = da.agent_id
        FROM {PG_SCHEMA}.dim_agent da
        WHERE LOWER(da.agent_id) = LOWER(t.agent_id)
          AND t.agent_id IS DISTINCT FROM da.agent_id;
    """)
    with target_engine.begin() as conn:
        res = conn.execute(normalize_query)
        print(f"🔄 normalized agent_id casing: {res.rowcount} rows")

    return full_tbl  # ส่งคืนชื่อเต็ม พร้อม schema

# =========================
# 🚀 APPLY UPDATE TO FACT
# =========================
@op
def update_fact_from_temp(temp_table_name: str) -> int:
    """
    อัปเดต {schema}.fact_sales_quotation.agent_id จาก temp table
    โดย join {schema}.dim_agent เพื่อกัน FK violation
    """
    if not temp_table_name:
        print("⚠️ temp table name missing, skip update.")
        return 0

    update_query = text(f"""
        UPDATE {PG_SCHEMA}.fact_sales_quotation fsq
        SET agent_id = da.agent_id
        FROM {temp_table_name} dc
        JOIN {PG_SCHEMA}.dim_agent da
          ON LOWER(da.agent_id) = LOWER(dc.agent_id)
        WHERE fsq.quotation_num = dc.quotation_num
          AND fsq.agent_id IS DISTINCT FROM da.agent_id;
    """)
    with target_engine.begin() as conn:
        res = conn.execute(update_query)
        print(f"✅ fact_sales_quotation updated: {res.rowcount} rows")
        return res.rowcount or 0

# =========================
# 🧹 CLEANUP TEMP
# =========================
@op
def drop_dim_agent_temp(temp_table_name: str) -> None:
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
    temp_full = stage_dim_agent_temp(extract_agent_mapping())
    _ = update_fact_from_temp(temp_full)
    drop_dim_agent_temp(temp_full)

# =========================
# ▶️ LOCAL RUN (optional)
# =========================
# if __name__ == "__main__":
#     df_map = extract_agent_mapping()
#     tname = stage_dim_agent_temp(df_map)
#     updated = update_fact_from_temp(tname)
#     drop_dim_agent_temp(tname)
#     print(f"🎉 done. updated rows = {updated}")
