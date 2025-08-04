from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, update
import re
from sqlalchemy import create_engine, MetaData, Table, update
from sqlalchemy import text
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
def extract_dim_order_type_data():
    query = "SELECT quotation_num, order_type_id FROM dim_order_type"
    df = pd.read_sql(query, target_engine)

    return df

@op
def extract_fact_sales_quotation_for_order_type():
    query = "SELECT * FROM fact_sales_quotation"
    df = pd.read_sql(query, target_engine)
    df = df.drop(columns=['order_type_id', 'create_at', 'update_at'], errors='ignore')
    return df

@op
def merge_dim_order_type_to_sales(df_order_type: pd.DataFrame, df_sales: pd.DataFrame):
    df_merged = pd.merge(df_order_type, df_sales, on='quotation_num', how='right')
    return df_merged

@op
def update_dim_order_type_in_sales(df_merged: pd.DataFrame):
    metadata = MetaData()
    table = Table('fact_sales_quotation', metadata, autoload_with=target_engine)
    records = df_merged.to_dict(orient='records')
    chunk_size = 5000

    for start in range(0, len(records), chunk_size):
        end = start + chunk_size
        chunk = records[start:end]

        print(f"🔄 Updating chunk {start // chunk_size + 1}: records {start} to {end - 1}")

        with target_engine.begin() as conn:
            for record in chunk:
                if 'quotation_num' not in record or pd.isna(record['quotation_num']):
                    print(f"⚠️ Skip row: no quotation_num: {record}")
                    continue
                if 'order_type_id' not in record or pd.isna(record['order_type_id']):
                    print(f"⚠️ Skip row: no order_type_id: {record}")
                    continue

                stmt = (
                    update(table)
                    .where(table.c.quotation_num == record['quotation_num'])
                    .values(
                        order_type_id=record['order_type_id'],
                        update_at=datetime.now()
                    )
                )
                conn.execute(stmt)

    print("✅ Update order_type_id completed successfully.")

    # 🔄 ใช้ connection ใหม่ตรงนี้
    with target_engine.begin() as conn:
        # ลบ unique constraint ก่อน
        conn.execute(text("""ALTER TABLE dim_order_type DROP CONSTRAINT IF EXISTS unique_quotation_num"""))

        # ลบ quotation_num column
        result = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'dim_order_type'
            AND column_name = 'quotation_num'
        """))
        if result.fetchone():
            conn.execute(text("ALTER TABLE dim_order_type DROP COLUMN quotation_num"))

@job
def update_fact_sales_quotation_order_type_id():
    update_dim_order_type_in_sales(
        merge_dim_order_type_to_sales(
            extract_dim_order_type_data(),
            extract_fact_sales_quotation_for_order_type()
        )
    )
