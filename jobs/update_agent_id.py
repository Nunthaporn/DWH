from dagster import op, job
import pandas as pd
import numpy as np
import os
import time
import random
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from datetime import datetime

# ✅ Load .env
load_dotenv()

# ✅ DB Connections
# Source: MariaDB
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)

# Target: PostgreSQL
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_quotation_idcus():
    df = pd.read_sql("SELECT quo_num, id_cus FROM fin_system_select_plan", source_engine)
    df = df.rename(columns={'quo_num': 'quotation_num'})
    print(f"📦 df_plan: {df.shape}")
    return df

@op
def extract_fact_sales_quotation():
    df = pd.read_sql("SELECT * FROM fact_sales_quotation WHERE agent_id IS NULL", target_engine)
    df = df.drop(columns=['create_at', 'update_at', 'agent_id'], errors='ignore')
    print(f"📦 df_sales: {df.shape}")
    return df

@op
def extract_dim_agent():
    df = pd.read_sql("SELECT * FROM dim_agent", target_engine)
    df = df.drop(columns=['create_at', 'update_at'], errors='ignore')
    df = df.rename(columns={'agent_id': 'id_cus'})
    print(f"📦 df_agent: {df.shape}")
    return df

@op
def join_and_clean_agent_data(df_plan: pd.DataFrame, df_sales: pd.DataFrame, df_agent: pd.DataFrame):
    # join ตาม logic เดิม
    df_merge = pd.merge(df_plan, df_sales, on='quotation_num', how='right')
    df_merge = pd.merge(df_merge, df_agent, on='id_cus', how='inner')
    df_merge = df_merge.rename(columns={'id_contact': 'agent_id'})
    df_merge = df_merge[['quotation_num', 'agent_id']]

    # ทำความสะอาดข้อมูล
    df_merge = df_merge.dropna(subset=['quotation_num', 'agent_id'])
    df_merge = df_merge.drop_duplicates(subset=['quotation_num'])
    df_merge = df_merge.where(pd.notnull(df_merge), None)

    print(f"📦 df_merge(clean): {df_merge.shape}")
    return df_merge

@op
def update_agent_id(df_selected: pd.DataFrame):
    if df_selected.empty:
        print("ℹ️ No rows to update.")
        return

    # กันกรณีหลุดมา
    df_selected = df_selected.dropna(subset=["quotation_num", "agent_id"]).drop_duplicates(subset=["quotation_num"])

    with target_engine.begin() as conn:
        # ลดโอกาสแฮงค์และ deadlock
        conn.exec_driver_sql("SET lock_timeout = '3s'")
        conn.exec_driver_sql("SET deadlock_timeout = '200ms'")
        conn.exec_driver_sql("SET statement_timeout = '60s'")

        # 1) สร้าง temp table
        conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_agent_updates(
                quotation_num text PRIMARY KEY,
                agent_id uuid NOT NULL
            ) ON COMMIT DROP
        """)

        # 2) bulk insert ลง temp table
        df_selected.to_sql(
            "tmp_agent_updates",
            con=conn,              # ใช้ SQLAlchemy Connection โดยตรง
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000
        )

        # 3) set-based UPDATE ตัวเดียว
        update_sql = text("""
            UPDATE fact_sales_quotation f
            SET agent_id = t.agent_id::text,
                update_at = NOW()
            FROM tmp_agent_updates t
            WHERE f.quotation_num = t.quotation_num
            AND f.payment_plan_id IS NULL
            AND (f.agent_id IS NULL OR f.agent_id IS DISTINCT FROM t.agent_id::text)
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

@job
def update_fact_sales_quotation_agent_id():
    update_agent_id(
        join_and_clean_agent_data(
            extract_quotation_idcus(),
            extract_fact_sales_quotation(),
            extract_dim_agent()
        )
    )

# 👇 (ถ้าจะรันแบบสคริปต์เดี่ยว)
# if __name__ == "__main__":
#     df_quotation = extract_quotation_idcus()
#     df_fact = extract_fact_sales_quotation()
#     df_agent = extract_dim_agent()
#     df_joined = join_and_clean_agent_data(df_quotation, df_fact, df_agent)
#     update_agent_id(df_joined)
#     print("🎉 completed! Data upserted to agent_id.")
