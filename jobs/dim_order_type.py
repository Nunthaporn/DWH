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
    pk_column = ['type_insurance', 'order_type', 'work_type', 'key_channel', 'check_type']

    # ✅ กรองซ้ำจาก DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()

    # ✅ Load ข้อมูลเดิมจาก PostgreSQL
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    # ✅ แปลง dtype ให้ตรงกันระหว่าง df และ df_existing
    for col in pk_column:
        if col in df.columns and col in df_existing.columns:
            try:
                df[col] = df[col].astype(df_existing[col].dtype)
            except Exception:
                df[col] = df[col].astype(str)
                df_existing[col] = df_existing[col].astype(str)

    # ✅ สร้าง tuple key สำหรับเปรียบเทียบ
    df['pk_tuple'] = df[pk_column].apply(lambda row: tuple(row), axis=1)
    df_existing['pk_tuple'] = df_existing[pk_column].apply(lambda row: tuple(row), axis=1)

    # ✅ หาข้อมูลใหม่ที่ยังไม่มีในฐานข้อมูล
    new_keys = set(df['pk_tuple']) - set(df_existing['pk_tuple'])
    df_to_insert = df[df['pk_tuple'].isin(new_keys)].copy()

    # ✅ หาข้อมูลที่มีอยู่แล้ว และเปรียบเทียบว่าเปลี่ยนแปลงหรือไม่
    common_keys = set(df['pk_tuple']) & set(df_existing['pk_tuple'])
    df_common_new = df[df['pk_tuple'].isin(common_keys)].copy()
    df_common_old = df_existing[df_existing['pk_tuple'].isin(common_keys)].copy()

    df_common_new.set_index(pk_column, inplace=True)
    df_common_old.set_index(pk_column, inplace=True)

    df_common_new = df_common_new.sort_index()
    df_common_old = df_common_old.sort_index()

    df_diff_mask = ~(df_common_new.eq(df_common_old, axis=1).all(axis=1))
    df_diff = df_common_new[df_diff_mask].reset_index()

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff)} rows")

    # ✅ Load table metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert
    if not df_to_insert.empty:
        df_to_insert = df_to_insert.drop(columns=['pk_tuple'])
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna().all(axis=1)].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} insert rows with null keys")
        if not df_to_insert_valid.empty:
            with target_engine.begin() as conn:
                conn.execute(metadata.insert(), df_to_insert_valid.to_dict(orient='records'))

    # ✅ Update
    if not df_diff.empty:
        with target_engine.begin() as conn:
            for record in df_diff.to_dict(orient='records'):
                stmt = pg_insert(metadata).values(**record)
                update_columns = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns
                    if c.name not in pk_column
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=pk_column,
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")
    
@job
def dim_order_type_etl():
    load_order_type_data(clean_order_type_data(extract_order_type_data()))
