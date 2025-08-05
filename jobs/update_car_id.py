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
def extract_dim_car_data():
    query = "SELECT quotation_num, car_sk FROM dim_car"
    df = pd.read_sql(query, target_engine)
    df = df.rename(columns={"car_sk": "car_id"})
    return df

@op
def extract_fact_sales_quotation_for_car():
    query = "SELECT * FROM fact_sales_quotation WHERE car_id IS NULL"
    df = pd.read_sql(query, target_engine)
    df = df.drop(columns=['car_id', 'create_at', 'update_at'], errors='ignore')
    return df

@op
def merge_car_to_sales(df_car: pd.DataFrame, df_sales: pd.DataFrame):
    df_merged = pd.merge(df_car, df_sales, on='quotation_num', how='right')
    return df_merged

@op
def update_car_id_in_sales(df_merged: pd.DataFrame):
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
                if 'car_id' not in record or pd.isna(record['car_id']):
                    print(f"⚠️ Skip row: no car_id: {record}")
                    continue

                stmt = (
                    update(table)
                    .where(table.c.quotation_num == record['quotation_num'])
                    .where(table.c.payment_plan_id.is_(None))
                    .values(
                        car_id=record['car_id'],
                        update_at=datetime.now()
                    )
                )
                conn.execute(stmt)

    print("✅ Update car_id completed successfully.")

    with conn.begin():
        result = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'dim_car'
            AND column_name = 'quotation_num'
        """))
        if result.fetchone():
            conn.execute(text("ALTER TABLE dim_car DROP COLUMN quotation_num"))

@job
def update_fact_sales_quotation_car_id():
    update_car_id_in_sales(
        merge_car_to_sales(
            extract_dim_car_data(),
            extract_fact_sales_quotation_for_car()
        )
    )
