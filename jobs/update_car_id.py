from dagster import op, job
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, update, text, bindparam
from datetime import datetime

# ✅ Load .env
load_dotenv()

# ✅ DB target (PostgreSQL)
target_user = os.getenv('DB_USER_test')
target_password = os.getenv('DB_PASSWORD_test')
target_host = os.getenv('DB_HOST_test')
target_port = os.getenv('DB_PORT_test')
target_db = 'fininsurance'

target_engine = create_engine(
    f"postgresql+psycopg2://{target_user}:{target_password}@{target_host}:{target_port}/{target_db}"
)

@op
def extract_dim_car_data():
    query = "SELECT quotation_num, car_sk FROM dim_car"
    df = pd.read_sql(query, target_engine).rename(columns={"car_sk": "car_id"})
    print(f"📦 df_car: {df.shape}")
    return df

@op
def extract_fact_sales_quotation_for_car():
    query = "SELECT * FROM fact_sales_quotation WHERE car_id IS NULL"
    df = pd.read_sql(query, target_engine).drop(columns=['car_id', 'create_at', 'update_at'], errors='ignore')
    print(f"📦 df_sales (car_id IS NULL): {df.shape}")
    return df

@op
def merge_car_to_sales(df_car: pd.DataFrame, df_sales: pd.DataFrame):
    df_merged = pd.merge(df_car, df_sales, on='quotation_num', how='right')
    print(f"📦 df_merged: {df_merged.shape}")
    return df_merged

@op
def update_car_id_in_sales(df_merged: pd.DataFrame):
    # เตรียมข้อมูลให้พร้อมอัปเดต: ต้องมีทั้ง quotation_num และ car_id และไม่เป็น NaN
    df_ready = (
        df_merged[['quotation_num', 'car_id']]
        .dropna(subset=['quotation_num', 'car_id'])
        .astype({'quotation_num': str})
    )
    records = df_ready.to_dict(orient='records')
    print(f"✅ ready-to-update rows: {len(records)}")

    if not records:
        print("ℹ️ ไม่มีรายการให้อัปเดต")
    else:
        metadata = MetaData()
        table = Table('fact_sales_quotation', metadata, autoload_with=target_engine)

        # ใช้ bindparam เพื่อทำ executemany ทีละ chunk
        stmt = (
            update(table)
            .where(table.c.quotation_num == bindparam('quotation_num'))
            .where(table.c.car_id.is_(None))  # กันการเขียนทับค่าที่มีอยู่
            .values(
                car_id=bindparam('car_id'),
                update_at=datetime.utcnow()
            )
        )

        chunk_size = 5000
        for start in range(0, len(records), chunk_size):
            end = start + chunk_size
            chunk = records[start:end]
            print(f"🔄 Updating chunk {start // chunk_size + 1}: rows {start}..{end-1}")
            with target_engine.begin() as conn:
                conn.execute(stmt, chunk)  # executemany

        print("✅ Update car_id completed successfully.")

    # ดรอปคอลัมน์ใน dim_car ด้วย transaction แยกใหม่ (อย่าใช้ conn เดิม)
    with target_engine.begin() as conn:
        conn.execute(text("ALTER TABLE dim_car DROP COLUMN IF EXISTS quotation_num"))

@job
def update_fact_sales_quotation_car_id():
    update_car_id_in_sales(
        merge_car_to_sales(
            extract_dim_car_data(),
            extract_fact_sales_quotation_for_car()
        )
    )
