from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

# =========================
# 🔧 ENV & DB CONNECTIONS
# =========================
load_dotenv()
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")

# MariaDB (source)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True
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
        # 👇 ตั้ง search_path + statement_timeout (จะปิดเฉพาะช่วง UPDATE ด้วย SET LOCAL)
        "options": f"-c search_path={PG_SCHEMA} -c statement_timeout=300000"
    },
    pool_pre_ping=True
)

# =========================
# 🔧 HELPERS
# =========================
def base_id_series(s: pd.Series) -> pd.Series:
    """คืนค่า agent_id ที่ตัด suffix '-defect' ออกสำหรับทำคีย์กลาง"""
    return s.astype(str).str.strip().str.replace(r"-defect$", "", regex=True)

def normalize_str_col(s: pd.Series) -> pd.Series:
    s = s.astype("string")
    s = s.str.strip()
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
    # 1) fin_system_pay (source)
    df_career = pd.read_sql(text("SELECT quo_num, id_cus FROM fin_system_pay"), source_engine)
    df_career = df_career.rename(columns={"id_cus": "agent_id", "quo_num": "quotation_num"})
    df_career["agent_id"] = normalize_str_col(df_career["agent_id"])

    # 2) quotation ทั้งหมดใน fact_sales_quotation (target)
    df_fact = pd.read_sql(text(f"SELECT quotation_num FROM {PG_SCHEMA}.fact_sales_quotation"), target_engine)

    # 3) right-join เพื่อให้มีทุก quotation แม้ไม่มีใน pay
    df_m1 = pd.merge(df_career, df_fact, on="quotation_num", how="right")

    # 4) มิติ agent (target) เพื่อทำ standardization
    df_main = pd.read_sql(text(f"SELECT agent_id FROM {PG_SCHEMA}.dim_agent"), target_engine)
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
        suffixes=("_m1", "_main"),
        indicator=False,
    )

    # 7) เลือก agent_id_final
    df_join["agent_id_final"] = np.where(
        df_join["agent_id_main"].notna(), df_join["agent_id_main"], df_join["agent_id_m1"]
    )

    # 8) คืนเฉพาะคอลัมน์ที่ต้องใช้
    df_out = df_join[["quotation_num", "agent_id_final"]].rename(columns={"agent_id_final": "agent_id"})
    df_out["agent_id"] = normalize_str_col(df_out["agent_id"])

    # กันซ้ำ quotation_num โดยให้แถวที่ agent_id ไม่ว่างอยู่ก่อน
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
    สร้างตารางชั่วคราว {PG_SCHEMA}.dim_agent_temp (quotation_num, agent_id)
    และ normalize ตัวสะกด agent_id ตาม {PG_SCHEMA}.dim_agent (case-insensitive)
    พร้อมสร้างดัชนีที่จำเป็น + ANALYZE
    """
    if df_map.empty:
        df_map = pd.DataFrame({
            "quotation_num": pd.Series(dtype="string"),
            "agent_id": pd.Series(dtype="string")
        })

    df_tmp = df_map.copy()
    df_tmp["quotation_num"] = normalize_str_col(df_tmp["quotation_num"])
    df_tmp["agent_id"] = normalize_str_col(df_tmp["agent_id"])

    # 👇 ใส่ schema ให้ชัดเจน
    df_tmp.to_sql(
        "dim_agent_temp",
        target_engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=20_000,
        schema=PG_SCHEMA,
    )
    print(f"✅ staged to {PG_SCHEMA}.dim_agent_temp: {len(df_tmp):,} rows")

    # 👇 normalize casing ให้ตรงกับ dim_agent
    normalize_query = text(f"""
        UPDATE {PG_SCHEMA}.dim_agent_temp t
        SET agent_id = da.agent_id
        FROM {PG_SCHEMA}.dim_agent da
        WHERE LOWER(da.agent_id) = LOWER(t.agent_id)
          AND t.agent_id IS DISTINCT FROM da.agent_id;
    """)

    # 🆕 สร้าง indexes (ถาวรและชั่วคราว) + ANALYZE
    # ddl = f"""
    # DO $$
    # BEGIN
    #     -- ถ้ายังไม่มี index ฝั่งถาวร ให้สร้าง (ไม่ error ถ้ามีอยู่แล้ว)
    #     EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fsq_quotation_num ON {PG_SCHEMA}.fact_sales_quotation(quotation_num)';
    #     EXECUTE 'CREATE INDEX IF NOT EXISTS idx_dim_agent_agent_id ON {PG_SCHEMA}.dim_agent(agent_id)';
    # END$$;

    # CREATE INDEX IF NOT EXISTS idx_dim_agent_temp_q ON {PG_SCHEMA}.dim_agent_temp(quotation_num);
    # CREATE INDEX IF NOT EXISTS idx_dim_agent_temp_a ON {PG_SCHEMA}.dim_agent_temp(agent_id);

    # ANALYZE {PG_SCHEMA}.dim_agent_temp;
    # """

    # with target_engine.begin() as conn:
    #     res = conn.execute(normalize_query)
    #     print(f"🔄 normalized agent_id casing: {res.rowcount} rows")
    #     conn.execute(text(ddl))
    #     print("🔧 indexes ready + ANALYZE on temp done")

    return f"{PG_SCHEMA}.dim_agent_temp"

# =========================
# 🚀 APPLY UPDATE TO FACT
# =========================
@op
def update_fact_from_temp(temp_table_name: str) -> int:
    """
    อัปเดต {PG_SCHEMA}.fact_sales_quotation.agent_id จาก temp table (จับคู่ quotation_num)
    โดย **ไม่ต้อง join dim_agent อีก** เพราะ agent_id ใน temp ถูก normalize แล้ว
    ปิด statement_timeout เฉพาะทรานแซกชันนี้ และอัปเดตเฉพาะแถวที่เปลี่ยนจริง
    """
    if not temp_table_name:
        print("⚠️ temp table name missing, skip update.")
        return 0

    if "." not in temp_table_name:
        temp_table_name = f"{PG_SCHEMA}.{temp_table_name}"

    update_sql = f"""
        -- ปิด timeout เฉพาะทรานแซกชันนี้
        SET LOCAL statement_timeout = 0;

        WITH cand AS (
            SELECT fsq.ctid AS fsq_ctid, dc.agent_id AS new_agent
            FROM {PG_SCHEMA}.fact_sales_quotation fsq
            JOIN {temp_table_name} dc
              ON fsq.quotation_num = dc.quotation_num
            WHERE fsq.agent_id IS DISTINCT FROM dc.agent_id
        )
        UPDATE {PG_SCHEMA}.fact_sales_quotation fsq
        SET agent_id = cand.new_agent
        FROM cand
        WHERE fsq.ctid = cand.fsq_ctid;
    """
    with target_engine.begin() as conn:
        res = conn.execute(text(update_sql))
        updated = res.rowcount or 0

    print(f"✅ fact_sales_quotation updated: {updated} rows")
    return updated

# =========================
# 🧹 CLEANUP TEMP
# =========================
@op
def drop_dim_agent_temp(temp_table_name: str) -> None:
    if not temp_table_name:
        return
    if "." not in temp_table_name:
        temp_table_name = f"{PG_SCHEMA}.{temp_table_name}"
    with target_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))
    print(f"🗑️ dropped {temp_table_name}")

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
if __name__ == "__main__":
    df_map = extract_agent_mapping()
    tname = stage_dim_agent_temp(df_map)
    updated = update_fact_from_temp(tname)
    drop_dim_agent_temp(tname)
    print(f"🎉 done. updated rows = {updated}")
