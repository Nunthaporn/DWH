# %%
from dagster import op, job
import pandas as pd
import numpy as np
import os
import time
import random
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# ✅ Load .env
load_dotenv()

# ✅ DB Connections
# Source: MariaDB
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)

# Target: PostgreSQL
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c statement_timeout=300000"  # 5 นาที
    },
    pool_pre_ping=True
)

# ---------- helpers ----------
def base_id_series(s: pd.Series) -> pd.Series:
    """คืน base id โดยตัดคำลงท้าย '-defect' และ trim ช่องว่าง"""
    return s.astype(str).str.strip().str.replace(r'-defect$', '', regex=True)

# ---------- EXTRACT ----------
@op
def extract_quotation_idcus_from_pay() -> pd.DataFrame:
    """ดึง quo_num, id_cus จาก fin_system_pay (ฝั่ง source)"""
    q = text("SELECT quo_num, id_cus FROM fin_system_pay")
    df = pd.read_sql(q, source_engine)
    df = df.rename(columns={'quo_num': 'quotation_num', 'id_cus': 'agent_id'})
    # strip เบื้องต้น
    df['agent_id'] = df['agent_id'].astype(str).str.strip()
    print(f"📦 df_pay (quo_num,id_cus): {df.shape}")
    return df

@op
def extract_fact_sales_quotation_null() -> pd.DataFrame:
    """ดึงเฉพาะ quotation_num ที่ agent_id ยังเป็น NULL (ฝั่ง target)"""
    q = text("SELECT quotation_num FROM fact_sales_quotation")
    df = pd.read_sql(q, target_engine)
    print(f"📦 df_fsq_null: {df.shape}")
    return df

@op
def extract_dim_agent_for_normalization() -> pd.DataFrame:
    """
    ดึง agent_id ทั้งหมดจาก dim_agent (ฝั่ง target)
    ใช้เพื่อ normalize mapping (เช่น กรณีมีทั้ง base และ base-defect)
    """
    q = text("SELECT agent_id FROM dim_agent")
    df = pd.read_sql(q, target_engine)
    print(f"📦 df_dim_agent: {df.shape}")
    return df

# ---------- TRANSFORM / JOIN ----------
@op
def normalize_and_join(
    df_pay: pd.DataFrame,
    df_fsq_null: pd.DataFrame,
    df_dim_agent: pd.DataFrame
) -> pd.DataFrame:
    """
    1) กรองเฉพาะ quotation_num ที่ agent_id ใน FSQ ยังเป็น NULL
    2) Normalize agent_id โดยใช้ base (ตัด -defect)
       - รวมกลุ่ม agent_id จาก dim_agent ด้วย base เดียวกัน
       - ถ้ามีทั้ง non-defect และ defect ให้เลือกตัวที่ลงท้าย -defect (prefer defect)
    3) แม็ป agent_id ของฝั่ง pay ให้เป็น agent_id ที่ normalize แล้ว
    """
    # --- เตรียมฝั่ง dim_agent: เลือกตัวแทนต่อ base (prefer -defect) ---
    dm = df_dim_agent.copy()
    dm['__base'] = base_id_series(dm['agent_id'])
    dm['__is_defect'] = dm['agent_id'].str.contains(r'-defect$', case=False, na=False)

    dup_mask = dm['__base'].duplicated(keep=False)
    main_single = dm[~dup_mask].copy()  # base ไม่ซ้ำ
    main_dups = (
        dm[dup_mask]
        .sort_values(['__base', '__is_defect'])  # non-defect ก่อน, defect หลัง
        .drop_duplicates('__base', keep='last')   # เก็บ defect ถ้ามี
    )
    dm_norm = pd.concat([main_single, main_dups], ignore_index=True)
    dm_norm = dm_norm[['agent_id', '__base']].rename(columns={'agent_id': 'agent_id_main'})

    # --- เตรียมฝั่ง pay + fsq(null) ---
    m1 = pd.merge(
        df_pay[['quotation_num', 'agent_id']],
        df_fsq_null[['quotation_num']],
        on='quotation_num',
        how='right'
    )
    # คีย์กลางสำหรับแม็ป
    m1['__base'] = base_id_series(m1['agent_id'])

    # --- แม็ปรวมกัน ---
    merged = pd.merge(
        m1,
        dm_norm,
        on='__base',
        how='left',
        suffixes=('_pay', '_main')
    )

    # agent_id_final: ถ้าแม็ปกับ main ได้ -> ใช้ agent_id_main, ไม่งั้นใช้ของ pay เดิม
    merged['agent_id_final'] = np.where(
        merged['agent_id_main'].notna(), merged['agent_id_main'], merged['agent_id']
    )

    # เคลียร์คอลัมน์ช่วย
    cols_keep = ['quotation_num', 'agent_id_final']
    out = merged[cols_keep].rename(columns={'agent_id_final': 'agent_id'})

    # ทำความสะอาด & ลดซ้ำ
    out = out.replace(['None', 'none', 'nan', 'NaN', 'NaT', ''], pd.NA)
    out = out.dropna(subset=['quotation_num', 'agent_id'])
    out['quotation_num'] = out['quotation_num'].astype('string').str.strip()
    out['agent_id'] = out['agent_id'].astype('string').str.strip()
    out = out.drop_duplicates(subset=['quotation_num'])

    print(f"📦 df_update_candidates (clean): {out.shape}")
    return out

# ---------- LOAD (UPDATE) ----------
@op
def update_agent_id(df_selected: pd.DataFrame) -> None:
    if df_selected.empty:
        print("ℹ️ No rows to update.")
        return

    # กันกรณีหลุดมา
    df_selected = (
      df_selected
      .dropna(subset=["quotation_num", "agent_id"])
      .drop_duplicates(subset=["quotation_num"])
    )

    with target_engine.begin() as conn:
        # ลดโอกาสแฮงค์และ deadlock
        conn.exec_driver_sql("SET lock_timeout = '3s'")
        conn.exec_driver_sql("SET deadlock_timeout = '200ms'")
        conn.exec_driver_sql("SET statement_timeout = '60s'")

        # 1) สร้าง temp table (ใช้ TEXT เพราะ agent_id อาจมี '-defect')
        conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_agent_updates(
                quotation_num text PRIMARY KEY,
                agent_id text NOT NULL
            ) ON COMMIT DROP
        """)

        # 2) bulk insert ลง temp table
        df_selected.to_sql(
            "tmp_agent_updates",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000
        )

        # 3) UPDATE จริง (อัปเดตเฉพาะที่เปลี่ยน/ยังว่าง)
        update_sql = text("""
            UPDATE fact_sales_quotation f
            SET agent_id = d.agent_id,
                update_at = NOW()
            FROM tmp_agent_updates t
            JOIN dim_agent d
            ON lower(d.agent_id) = lower(t.agent_id)   -- join แบบ lowercase
            WHERE f.quotation_num = t.quotation_num
            AND (f.agent_id IS NULL OR f.agent_id IS DISTINCT FROM d.agent_id);
        """)

        # 4) ใส่ retry เมื่อเจอ deadlock
        max_retries = 5
        for attempt in range(max_retries):
            try:
                result = conn.execute(update_sql)
                print(f"✅ Updated rows: {result.rowcount}")
                print("✅ Update agent_id completed successfully.")
                break
            except OperationalError as e:
                msg = str(getattr(e, "orig", e)).lower()
                if "deadlock detected" in msg and attempt < max_retries - 1:
                    sleep_s = (2 ** attempt) + random.random()
                    print(f"⚠️ Deadlock detected. Retrying in {sleep_s:.2f}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(sleep_s)
                    continue
                raise

# ---------- JOB ----------
@job
def update_fact_sales_quotation_agent_id():
    update_agent_id(
        normalize_and_join(
            extract_quotation_idcus_from_pay(),
            extract_fact_sales_quotation_null(),
            extract_dim_agent_for_normalization()
        )
    )

if __name__ == "__main__":
    df_pay = extract_quotation_idcus_from_pay()
    df_fsq = extract_fact_sales_quotation_null()
    df_dim = extract_dim_agent_for_normalization()
    df_join = normalize_and_join(df_pay, df_fsq, df_dim)
    update_agent_id(df_join)
    print("🎉 completed! agent_id updated.")
