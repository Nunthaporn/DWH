from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Table, MetaData, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ✅ Load environment variables
load_dotenv()

# ✅ Source: MariaDB
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)
source_engine_task  = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task"
)
# ✅ Target: PostgreSQL
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_order_type_data():
    query_plan = """
        SELECT quo_num, type_insure, type_work, type_status, type_key, app_type, chanel_key
        FROM fin_system_select_plan
        WHERE datestart >= '2025-01-01' AND datestart < '2025-07-01'
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    query_order = """
        SELECT quo_num, worksend
        FROM fin_order
    """
    df_plan = pd.read_sql(query_plan, source_engine)
    df_order = pd.read_sql(query_order, source_engine_task)

    df_merged = pd.merge(df_plan, df_order, on='quo_num', how='left')
    return df_merged

@op
def clean_order_type_data(df: pd.DataFrame):
    def fill_chanel_key(row):
        chanel_key = row['chanel_key']
        type_key = row['type_key']
        app_type = row['app_type']
        type_insure = row['type_insure']

        if pd.notnull(chanel_key) and str(chanel_key).strip() != "":
            return chanel_key
        if pd.notnull(type_key) and pd.notnull(app_type):
            if type_key == app_type:
                return f"{type_key} VIF" if type_insure == 'ตรอ' else type_key
            if type_key in app_type:
                base = app_type.replace(type_key, "").replace("-", "").strip()
                return f"{type_key} {base}" if base else type_key
            if app_type in type_key:
                base = type_key.replace(app_type, "").replace("-", "").strip()
                return f"{app_type} {base}" if base else app_type
            return f"{type_key} {app_type}"
        if pd.notnull(type_key):
            return f"{type_key} {type_insure}" if type_insure else type_key
        if pd.notnull(app_type):
            return f"{app_type} {type_insure}" if type_insure else app_type
        return ''

    df['chanel_key'] = df.apply(fill_chanel_key, axis=1)

    df['chanel_key'] = df['chanel_key'].replace({
        'B2B': 'APP B2B',
        'WEB ตรอ': 'WEB VIF',
        'TELE': 'APP TELE',
        'APP ประกันรถ': 'APP B2B',
        'WEB ประกันรถ': 'WEB'
    })

    df.drop(columns=['type_key', 'app_type'], inplace=True)

    df.rename(columns={
        "quo_num": "quotation_num",
        "type_insure": "type_insurance",
        "type_work": "order_type",
        "type_status": "check_type",
        "worksend": "work_type",
        "chanel_key": "key_channel"
    }, inplace=True)

    df['key_channel'] = df['key_channel'].astype(str).str.strip().str.replace(r'\s+', '-', regex=True)
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df.replace("NaN", np.nan, inplace=True)
    df.drop_duplicates(subset=['quotation_num'], keep='first', inplace=True)

    return df

@op
def load_order_type_data(df: pd.DataFrame):
    table_name = 'dim_order_type'
    pk_column = 'quotation_num'

    with target_engine.begin() as conn:
        conn.execute(text(f"""
            ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS quotation_num VARCHAR(255);
        """))

    insp = inspect(target_engine)
    columns = [col['name'] for col in insp.get_columns(table_name)]

    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    with target_engine.begin() as conn:
        for record in df.to_dict(orient='records'):
            if not record.get(pk_column):
                continue
            stmt = pg_insert(metadata).values(**record)
            update_columns = {c.name: stmt.excluded[c.name] for c in metadata.columns if c.name != pk_column}
            stmt = stmt.on_conflict_do_update(index_elements=[pk_column], set_=update_columns)
            conn.execute(stmt)

    print(f"✅ Inserted/Updated: {len(df)} rows")

@job
def dim_order_type_etl():
    load_order_type_data(clean_order_type_data(extract_order_type_data()))

if __name__ == "__main__":
    df_raw = extract_order_type_data()
    print("✅ Extracted logs:", df_raw.shape)

    df_clean = clean_order_type_data((df_raw))
    print("✅ Cleaned columns:", df_clean.columns)

    # print(df_clean.head(10))

    # output_path = "dim_order_type.csv"
    # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
    # print(f"💾 Saved to {output_path}")

    # output_path = "dim_order_type.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_order_type_data(df_clean)
    print("🎉 Test completed! Data upserted to dim_order_type.")