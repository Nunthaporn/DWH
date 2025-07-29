from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ✅ Load environment variables
load_dotenv()

# ✅ DB source: MariaDB
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)

# ✅ DB target: PostgreSQL
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_card_agent_data() -> pd.DataFrame:
    query = """
        SELECT 
            ic_ins.cuscode AS agent_id,
            ic_ins.title,
            CONCAT(ic_ins.name, ' ', ic_ins.lastname) AS agent_name,

            ic_ins.card_no AS id_card_ins,
            ic_ins.type AS type_ins,
            ic_ins.revoke_type_code AS revoke_type_ins,
            STR_TO_DATE(NULLIF(ic_ins.create_date, '0000-00-00'), '%Y-%m-%d') AS card_issued_date_ins,
            STR_TO_DATE(NULLIF(ic_ins.expire_date, '0000-00-00'), '%Y-%m-%d') AS card_expiry_date_ins,

            ic_life.card_no AS id_card_life,
            ic_life.type AS type_life,
            ic_life.revoke_type_code AS revoke_type_life,
            STR_TO_DATE(NULLIF(ic_life.create_date, '0000-00-00'), '%Y-%m-%d') AS card_issued_date_life,
            STR_TO_DATE(NULLIF(ic_life.expire_date, '0000-00-00'), '%Y-%m-%d') AS card_expiry_date_life

        FROM tbl_ins_card ic_ins
        LEFT JOIN tbl_ins_card ic_life
            ON ic_life.cuscode = ic_ins.cuscode AND ic_life.ins_type = 'LIFE'
        WHERE ic_ins.ins_type = 'INS'
            AND ic_ins.cuscode LIKE 'FNG%'
            AND ic_ins.name NOT LIKE '%ทดสอบ%'
            AND ic_ins.name NOT LIKE '%test%'
            AND ic_ins.name NOT LIKE '%เทสระบบ%'
            AND ic_ins.name NOT LIKE '%Tes ระบบ%'
            AND ic_ins.name NOT LIKE '%ทด่ท%'
            AND ic_ins.name NOT LIKE '%ทด สอบ%'
    """
    df = pd.read_sql(query, source_engine)
    return df

@op
def clean_card_agent_data(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    df = df.replace('NaN', np.nan)
    return df

@op
def load_card_agent_data(df: pd.DataFrame):
    table_name = 'dim_card_agent'
    pk_column = 'agent_id'

    df = df[df[pk_column].notna()].copy()
    df = df[~df[pk_column].duplicated(keep='first')]

    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT {pk_column} FROM {table_name}", conn)
        existing_ids = set(df_existing[pk_column])
    
    df_to_insert = df[~df[pk_column].isin(existing_ids)].copy()

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=target_engine)

    if not df_to_insert.empty:
        with target_engine.begin() as conn:
            conn.execute(table.insert(), df_to_insert.to_dict(orient='records'))
        print(f"✅ Inserted {len(df_to_insert)} new rows.")
    else:
        print("ℹ️ No new rows to insert.")

@job
def dim_card_agent_etl():
    load_card_agent_data(clean_card_agent_data(extract_card_agent_data()))
