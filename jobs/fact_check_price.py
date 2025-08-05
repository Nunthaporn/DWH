from dagster import op, job, in_process_executor
import pandas as pd
import numpy as np
import os
import json
import re
import gc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime

# ตั้งค่า logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# DB: Source (MariaDB)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600
)

# DB: Target (PostgreSQL)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600
)

@op
def extract_logs_data():
    try:
        logger.info("\ud83d\udcca Extracting logs data...")
        query_logs = """
            SELECT cuscode, brand, series, subseries, year, no_car, type, repair_type,
                   assured_insurance_capital1, camera, addon, quo_num, create_at, results, selected, carprovince
            FROM fin_customer_logs_B2B
            WHERE create_at BETWEEN '2024-01-01' AND '2024-04-30'
            ORDER BY create_at
        """
        chunks_logs = []
        for chunk in pd.read_sql(query_logs, source_engine, chunksize=1000):
            chunks_logs.append(chunk)
            if len(chunks_logs) % 5 == 0:
                gc.collect()
        return pd.concat(chunks_logs, ignore_index=True)
    except Exception as e:
        logger.error(f"\u274c Error in extract_logs_data: {str(e)}")
        raise

@op
def extract_checkprice_data():
    try:
        logger.info("\ud83d\udcca Extracting checkprice data...")
        query_check = """
            SELECT id_cus, datekey, brand, model, submodel, yearcar, idcar, nocar, type_ins,
                   company, tunprakan, deduct, status, type_driver, type_camera, type_addon, status_send
            FROM fin_checkprice
            ORDER BY datekey
        """
        chunks_check = []
        for chunk in pd.read_sql(query_check, source_engine, chunksize=1000):
            chunks_check.append(chunk)
            if len(chunks_check) % 5 == 0:
                gc.collect()
        return pd.concat(chunks_check, ignore_index=True)
    except Exception as e:
        logger.error(f"\u274c Error in extract_checkprice_data: {str(e)}")
        raise

@op
def clean_fact_check_price(df_logs: pd.DataFrame, df_checkprice: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("\ud83e\uddf9 Cleaning data...")
        logs = df_logs.copy()
        check = df_checkprice.copy()

        logs.rename(columns={
            'cuscode': 'id_cus', 'series': 'model', 'subseries': 'submodel', 'year': 'yearcar',
            'no_car': 'car_code', 'assured_insurance_capital1': 'sum_insured',
            'camera': 'type_camera', 'addon': 'type_addon', 'quo_num': 'select_quotation',
            'create_at': 'transaction_date', 'carprovince': 'province_car'
        }, inplace=True)
        logs['type_insurance'] = 'ชั้น' + logs['type'].astype(str) + logs['repair_type'].astype(str)
        logs['input_type'] = 'auto'
        logs.drop(columns=['type', 'repair_type', 'results', 'selected'], inplace=True)

        check.rename(columns={
            'datekey': 'transaction_date', 'nocar': 'car_code', 'type_ins': 'type_insurance',
            'tunprakan': 'sum_insured', 'deduct': 'deductible'
        }, inplace=True)
        check['input_type'] = 'manual'
        # Drop columns that are not needed in the final table
        check.drop(columns=['idcar', 'company', 'status', 'type_driver', 'status_send'], inplace=True)

        df_combined = pd.concat([logs, check], ignore_index=True)
        df_combined['transaction_date'] = pd.to_datetime(df_combined['transaction_date'], errors='coerce')
        df_combined['transaction_date'] = df_combined['transaction_date'].dt.strftime('%Y%m%d').astype('Int64')
        df_combined = df_combined.drop_duplicates()
        logger.info(f"\u2705 Cleaned shape: {df_combined.shape}")
        logger.info(f"\u2705 Columns: {list(df_combined.columns)}")
        return df_combined
    except Exception as e:
        logger.error(f"\u274c Error in clean_fact_check_price: {str(e)}")
        raise

@op
def load_fact_check_price(df: pd.DataFrame):
    try:
        logger.info("\ud83d\udd10 Loading to DB...")
        table_name = 'fact_check_price_test'
        pk_column = ['id_cus', 'brand', 'model', 'submodel', 'yearcar', 'car_code',
                     'sum_insured', 'type_camera', 'type_addon', 'transaction_date']
        df = df.drop_duplicates(subset=pk_column)
        
        # Get table schema
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=target_engine)
        table_columns = [col.name for col in table.columns]
        logger.info(f"\u2705 Table columns: {table_columns}")
        
        # Ensure DataFrame columns match table columns
        df_columns = list(df.columns)
        missing_columns = [col for col in table_columns if col not in df_columns]
        extra_columns = [col for col in df_columns if col not in table_columns]
        
        if missing_columns:
            logger.warning(f"\u26a0 Missing columns in DataFrame: {missing_columns}")
        if extra_columns:
            logger.warning(f"\u26a0 Extra columns in DataFrame: {extra_columns}")
            df = df.drop(columns=extra_columns)

        with target_engine.begin() as conn:
            conn.execute(table.insert().values(df.to_dict(orient='records')))
        logger.info(f"\u2705 Inserted: {len(df)} rows")
    except Exception as e:
        logger.error(f"\u274c Error in load_fact_check_price: {str(e)}")
        raise

@job(executor_def=in_process_executor)
def fact_check_price_etl():
    load_fact_check_price(
        clean_fact_check_price(
            extract_logs_data(),
            extract_checkprice_data()
        )
    )

# if __name__ == "__main__":
#     logs = extract_logs_data()
#     check = extract_checkprice_data()
#     # print("✅ Extracted logs:", df_logs.shape)
#     # print("✅ Extracted checkprice:", df_checkprice.shape)

#     cleaned = clean_fact_check_price(logs, check)
#     # print("✅ Cleaned columns:", df_clean.columns)

#     # print(df_clean.head(10))

#     # output_path = "fact_check_price.xlsx"
#     # cleaned.to_excel(output_path, index=False, engine='openpyxl')
#     # print(f"💾 Saved to {output_path}")

#     load_fact_check_price(cleaned)
#     print("🎉 completed! Data upserted to fact_check_price.")
