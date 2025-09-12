from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
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
        "options": "-c statement_timeout=300000"
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
    ดึง agent จาก fin_system_pay (source) + quotation ทั้งหมดจาก fact_sales_quotation (target)
    แล้วยุบ agent_id ฝั่งมิติ (dim_agent) ให้เหลือ standard เดียวต่อ base_id
    แล้วเลือก agent_id ที่เหมาะสมสำหรับแต่ละ quotation_num
    """
    # 1) ดึงจาก source: fin_system_pay
    with source_engine.begin() as sconn:
        df_career = pd.read_sql(
            text("SELECT quo_num, id_cus FROM fin_system_pay"),
            sconn
        )
    df_career = df_career.rename(columns={"id_cus": "agent_id", "quo_num": "quotation_num"})
    df_career["agent_id"] = normalize_str_col(df_career["agent_id"])

    # 2) ดึง quotation ทั้งหมดใน fact_sales_quotation (target)
    with target_engine.begin() as tconn:
        df_fact = pd.read_sql(text("SELECT quotation_num FROM fact_sales_quotation"), tconn)

    # 3) right-join เพื่อให้มีทุก quotation แม้ไม่มีใน pay
    df_m1 = pd.merge(df_career, df_fact, on="quotation_num", how="right")

    # 4) มิติ agent (target) เพื่อทำ standardization
    with target_engine.begin() as tconn:
        df_main = pd.read_sql(text("SELECT agent_id FROM dim_agent"), tconn)
    df_main["agent_id"] = normalize_str_col(df_main["agent_id"]).dropna()

    # 4.1) ทำคีย์ base และเลือกตัวแทน/ตัว defect
    dfm = df_main.copy()
    dfm["__base"] = base_id_series(dfm["agent_id"])
    dfm["__is_defect"] = dfm["agent_id"].str.contains(r"-defect$", case=False, na=False)
    dup_mask = dfm["__base"].duplicated(keep=False)

    main_single = dfm[~dup_mask].copy()
    # sort: non-defect ก่อน, defect หลัง → keep='last' จะได้ defect ถ้ามี
    main_dups = (
        dfm[dup_mask]
        .sort_values(["__base", "__is_defect"])
        .drop_duplicates("__base", keep="last")
    )
    df_main_norm = pd.concat([main_single, main_dups], ignore_index=True)

    # 5) ทำคีย์กลางที่ด้าน mapping
    df_m1["__base"] = base_id_series(df_m1["agent_id"])

    # 6) จับคู่ด้วย base
    df_join = pd.merge(
        df_m1,
        df_main_norm.drop(columns=["__is_defect"], errors="ignore"),
        on="__base",
        how="left",
        suffixes=("_m1", "_main")
    )

    # 7) เลือก agent_id_final: ถ้าแมตช์มิติได้ใช้ของมิติ, ไม่งั้นใช้ของเดิม
    df_join["agent_id_final"] = np.where(
        df_join["agent_id_main"].notna(), df_join["agent_id_main"], df_join["agent_id_m1"]
    )

    # 8) คืนเฉพาะ 2 คอลัมน์ที่ต้องใช้ และ normalize อีกรอบ
    df_out = df_join[["quotation_num", "agent_id_final"]].rename(columns={"agent_id_final": "agent_id"})
    df_out["agent_id"] = normalize_str_col(df_out["agent_id"])

    # กันซ้ำ quotation_num โดยให้ค่าว่างไปท้าย
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
    สร้างตารางชั่วคราว dim_agent_temp (quotation_num, agent_id)
    และ normalize ตัวสะกด agent_id ตาม dim_agent (case-insensitive)
    """
    tbl = "dim_agent_temp"

    if df_map.empty:
        df_map = pd.DataFrame({
            "quotation_num": pd.Series(dtype="string"),
            "agent_id": pd.Series(dtype="string")
        })

    # sanitize
    df_tmp = df_map.copy()
    df_tmp["quotation_num"] = normalize_str_col(df_tmp["quotation_num"])
    df_tmp["agent_id"] = normalize_str_col(df_tmp["agent_id"])
    df_tmp = df_tmp.dropna(subset=["quotation_num"])  # กันแถวที่ไม่มี key

    with target_engine.begin() as conn:
        df_tmp.to_sql(tbl, con=conn, if_exists="replace", index=False, method="multi", chunksize=20_000)
        # ดัชนีชั่วคราวช่วยเร่ง UPDATE
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{tbl}_quo ON {tbl} (quotation_num)'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{tbl}_agent ON {tbl} (LOWER(agent_id))'))
    print(f"✅ staged to {tbl}: {len(df_tmp):,} rows")

    # Normalize casing ให้ตรง dim_agent
    normalize_query = text(f"""
        UPDATE {tbl} t
        SET agent_id = da.agent_id
        FROM dim_agent da
        WHERE LOWER(da.agent_id) = LOWER(t.agent_id)
          AND t.agent_id IS DISTINCT FROM da.agent_id;
    """)
    with target_engine.begin() as conn:
        res = conn.execute(normalize_query)
        print(f"🔄 normalized agent_id casing: {res.rowcount} rows")

    return tbl

# =========================
# 🚀 APPLY UPDATE TO FACT
# =========================
@op
def update_fact_from_temp(temp_table_name: str) -> int:
    """
    อัปเดต fact_sales_quotation.agent_id จาก temp table (จับคู่ quotation_num)
    และยืนยันอีกครั้งด้วย join dim_agent เพื่อกัน FK violation
    """
    if not temp_table_name:
        print("⚠️ temp table name missing, skip update.")
        return 0

    update_query = text(f"""
        UPDATE fact_sales_quotation fsq
        SET agent_id = da.agent_id
        FROM {temp_table_name} dc
        JOIN dim_agent da
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
    print("🗑️ dropped dim_agent_temp")

# =========================
# 🧱 DAGSTER JOB
# =========================
@job
def update_agent_id_on_fact():
    temp = stage_dim_agent_temp(extract_agent_mapping())
    _ = update_fact_from_temp(temp)
    drop_dim_agent_temp(temp)

# =========================
# ▶️ LOCAL RUN (optional)
# =========================
# if __name__ == "__main__":
#     df_map = extract_agent_mapping()
#     tname = stage_dim_agent_temp(df_map)
#     updated = update_fact_from_temp(tname)
#     drop_dim_agent_temp(tname)
#     print(f"🎉 done. updated rows = {updated}")
