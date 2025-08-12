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
    # 🔧 normalize key ในฝั่ง pandas ไว้ก่อน
    df['quotation_num'] = df['quotation_num'].astype(str).str.strip()
    df['id_cus'] = df['id_cus'].astype(str).str.strip()
    print(f"📦 df_plan: {df.shape}")
    return df

@op
def extract_fact_sales_quotation():
    # เอาเฉพาะที่ agent_id ยังว่าง
    df = pd.read_sql("SELECT quotation_num, payment_plan_id, agent_id FROM fact_sales_quotation WHERE agent_id IS NULL", target_engine)
    # 🔧 normalize key
    df['quotation_num'] = df['quotation_num'].astype(str).str.strip()
    print(f"📦 df_sales: {df.shape}")
    return df

@op
def extract_dim_agent():
    # dim_agent: มี agent_id (string/id_cus) และ id_contact (uuid)
    df = pd.read_sql("SELECT agent_id AS id_cus, id_contact FROM dim_agent", target_engine)
    df['id_cus'] = df['id_cus'].astype(str).str.strip()
    print(f"📦 df_agent: {df.shape}")
    return df

@op
def join_and_clean_agent_data(df_plan: pd.DataFrame, df_sales: pd.DataFrame, df_agent: pd.DataFrame):
    # plan + sales (เอาเฉพาะ quotation ที่อยู่ใน sales)
    df_merge = pd.merge(df_plan, df_sales, on='quotation_num', how='right')
    # join หา id_contact (uuid) ด้วย id_cus
    df_merge = pd.merge(df_merge, df_agent, on='id_cus', how='inner')  # id_contact มาจาก dim_agent
    df_merge = df_merge.rename(columns={'id_contact': 'agent_uuid'})
    df_merge = df_merge[['quotation_num', 'payment_plan_id', 'agent_uuid']]

    # ทำความสะอาด
    df_merge = df_merge.dropna(subset=['quotation_num', 'agent_uuid'])
    df_merge = df_merge.drop_duplicates(subset=['quotation_num'])
    # แปลงเป็น str ชัด ๆ
    df_merge['quotation_num'] = df_merge['quotation_num'].astype(str).str.strip()

    print(f"📦 df_merge(clean): {df_merge.shape}")
    return df_merge

@op
def update_agent_id(df_selected: pd.DataFrame, require_payment_plan_null: bool = True):
    if df_selected.empty:
        print("ℹ️ No rows to update (df_selected is empty).")
        return

    # กันหลุด
    df_selected = df_selected.dropna(subset=["quotation_num", "agent_uuid"]).drop_duplicates(subset=["quotation_num"])

    with target_engine.begin() as conn:
        # ลดโอกาสแฮงค์และ deadlock
        conn.exec_driver_sql("SET lock_timeout = '3s'")
        conn.exec_driver_sql("SET deadlock_timeout = '200ms'")
        conn.exec_driver_sql("SET statement_timeout = '60s'")

        # 1) temp table เป็น TEXT ทั้งคู่ เพื่อเลี่ยงปัญหา uuid ตอน insert
        conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_agent_updates(
                quotation_num text PRIMARY KEY,
                agent_uuid text NOT NULL
            ) ON COMMIT DROP
        """)

        # 2) bulk insert ลง temp table
        #    บังคับเป็น str + trim อีกรอบ
        df_tmp = df_selected.copy()
        df_tmp['quotation_num'] = df_tmp['quotation_num'].astype(str).str.strip()
        df_tmp['agent_uuid'] = df_tmp['agent_uuid'].astype(str).str.strip()
        df_tmp[['quotation_num', 'agent_uuid']].to_sql(
            "tmp_agent_updates",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000
        )

        # 3) debug: นับจำนวน row ใน temp
        tmp_count = conn.execute(text("SELECT COUNT(*) FROM tmp_agent_updates")).scalar()
        print(f"🧮 tmp rows: {tmp_count}")

        # 3.1) debug: นับจำนวนที่ match กับ fact ตาม key (และตามเงื่อนไข payment_plan ถ้ามี)
        match_sql = """
            SELECT COUNT(*)
            FROM fact_sales_quotation f
            JOIN tmp_agent_updates t
              ON btrim(f.quotation_num) = btrim(t.quotation_num)
        """
        if require_payment_plan_null:
            match_sql += " WHERE f.payment_plan_id IS NULL"
        matched = conn.execute(text(match_sql)).scalar()
        print(f"🧩 matched rows (pre-check): {matched}")

        if matched == 0:
            hint = "ตรวจสอบว่า quotation_num ตรงกันหรือมีช่องว่าง/ตัวพิมพ์ หรือ payment_plan_id ไม่เป็น NULL ทำให้ไม่เข้าเงื่อนไข"
            print(f"⚠️ No rows matched for update. {hint}")

        # 4) UPDATE แบบ set-based
        where_payment_plan = "AND f.payment_plan_id IS NULL" if require_payment_plan_null else ""
        update_sql = text(f"""
            UPDATE fact_sales_quotation f
            SET agent_id = (t.agent_uuid::uuid)::text,
                update_at = NOW()
            FROM tmp_agent_updates t
            WHERE btrim(f.quotation_num) = btrim(t.quotation_num)
            {where_payment_plan}
            AND (f.agent_id IS NULL OR f.agent_id IS DISTINCT FROM (t.agent_uuid::uuid)::text)
        """)

        max_retries = 5
        for attempt in range(max_retries):
            try:
                result = conn.execute(update_sql)
                print(f"✅ Updated rows: {result.rowcount}")
                if result.rowcount == 0 and not require_payment_plan_null:
                    print("ℹ️ Still 0 updated. อาจมี agent_id เท่ากันอยู่แล้ว หรือ key ไม่ตรง")
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
    df_joined = join_and_clean_agent_data(
        extract_quotation_idcus(),
        extract_fact_sales_quotation(),
        extract_dim_agent()
    )
    # ถ้าต้องการให้ไม่บังคับ payment_plan_id เป็น NULL ให้ส่ง require_payment_plan_null=False
    update_agent_id(df_joined, require_payment_plan_null=True)


# 👇 (ถ้าจะรันแบบสคริปต์เดี่ยว)
# if __name__ == "__main__":
#     df_quotation = extract_quotation_idcus()
#     df_fact = extract_fact_sales_quotation()
#     df_agent = extract_dim_agent()
#     df_joined = join_and_clean_agent_data(df_quotation, df_fact, df_agent)
#     update_agent_id(df_joined)
#     print("🎉 completed! Data upserted to agent_id.")
