from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, update

# ✅ Load .env
load_dotenv()

# ✅ DB connections
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_agent_quotation_mapping():
    df_plan = pd.read_sql("SELECT quo_num, id_cus FROM fin_system_select_plan", source_engine)
    df_plan = df_plan.rename(columns={'quo_num': 'quotation_num'})

    df_fact = pd.read_sql("SELECT * FROM fact_sales_quotation", target_engine)
    df_fact = df_fact.drop(columns=['create_at', 'update_at', 'agent_id'], errors='ignore')

    df_dim = pd.read_sql("SELECT * FROM dim_agent", target_engine)
    df_dim = df_dim.drop(columns=['create_at', 'update_at'], errors='ignore')
    df_dim = df_dim.rename(columns={'agent_id': 'id_cus'})

    df_merged = pd.merge(df_fact, df_plan, on='quotation_num', how='right')
    df_final = pd.merge(df_merged, df_dim, on='id_cus', how='inner')
    df_final = df_final.rename(columns={'id_contact': 'agent_id'})
    df_final = df_final[['quotation_num', 'agent_id']]
    df_final = df_final.dropna(subset=['quotation_num', 'agent_id'])
    return df_final

@op
def update_agent_ids(df_selected: pd.DataFrame):
    metadata = MetaData()
    table = Table('fact_sales_quotation', metadata, autoload_with=target_engine)
    records = df_selected.to_dict(orient='records')
    chunk_size = 5000

    for start in range(0, len(records), chunk_size):
        chunk = records[start:start + chunk_size]
        print(f"🔄 Updating chunk {start // chunk_size + 1}: records {start} to {start + len(chunk) - 1}")
        with target_engine.begin() as conn:
            for record in chunk:
                if pd.isna(record['quotation_num']) or pd.isna(record['agent_id']):
                    continue
                stmt = (
                    update(table)
                    .where(table.c.quotation_num == record['quotation_num'])
                    .values(agent_id=record['agent_id'])
                )
                conn.execute(stmt)
    print("✅ Update agent_id completed successfully.")

@job
def update_agent_id_etl():
    update_agent_ids(extract_agent_quotation_mapping())
