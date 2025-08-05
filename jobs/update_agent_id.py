from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, update
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
    return df

@op
def extract_fact_sales_quotation():
    df = pd.read_sql("SELECT * FROM fact_sales_quotation WHERE agent_id IS NULL", target_engine)
    df = df.drop(columns=['create_at', 'update_at', 'agent_id'], errors='ignore')
    return df

@op
def extract_dim_agent():
    df = pd.read_sql("SELECT * FROM dim_agent", target_engine)
    df = df.drop(columns=['create_at', 'update_at'], errors='ignore')
    df = df.rename(columns={'agent_id': 'id_cus'})
    return df

@op
def join_and_clean_agent_data(df_plan: pd.DataFrame, df_sales: pd.DataFrame, df_agent: pd.DataFrame):
    df_merge = pd.merge(df_plan, df_sales, on='quotation_num', how='right')
    df_merge = pd.merge(df_merge, df_agent, on='id_cus', how='inner')
    df_merge = df_merge.rename(columns={'id_contact': 'agent_id'})
    df_merge = df_merge[['quotation_num', 'agent_id']]
    df_merge = df_merge.where(pd.notnull(df_merge), None)  # Convert NaN, NaT -> None
    return df_merge

@op
def update_agent_id(df_selected: pd.DataFrame):
    metadata = MetaData()
    table = Table('fact_sales_quotation', metadata, autoload_with=target_engine)
    records = df_selected.to_dict(orient='records')
    chunk_size = 5000

    for start in range(0, len(records), chunk_size):
        end = start + chunk_size
        chunk = records[start:end]
        print(f"🔄 Updating chunk {start // chunk_size + 1}: records {start} to {end - 1}")

        with target_engine.begin() as conn:
            for record in chunk:
                if not record.get('quotation_num') or not record.get('agent_id'):
                    print(f"⚠️ Skip row: missing data: {record}")
                    continue
                stmt = (
                    update(table)
                    .where(table.c.quotation_num == record['quotation_num'])
                    .where(table.c.payment_plan_id.is_(None))
                    .values(
                        agent_id=record['agent_id'],
                        update_at=datetime.now()
                    )
                )
                conn.execute(stmt)

    print("✅ Update agent_id completed successfully.")

@job
def update_fact_sales_quotation_agent_id():
    update_agent_id(
        join_and_clean_agent_data(
            extract_quotation_idcus(),
            extract_fact_sales_quotation(),
            extract_dim_agent()
        )
    )
