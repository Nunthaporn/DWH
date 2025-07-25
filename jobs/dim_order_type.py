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
    df = df.applymap(lambda x: np.nan if isinstance(x, str) and x.strip().lower() == "nan" else x)
    df = df.where(pd.notnull(df), None)

    return df

@op
def load_order_type_data(df: pd.DataFrame):
    table_name = 'dim_order_type'
    pk_column = ['quotation_num']

    df = df[~df[pk_column].duplicated(keep='first')].copy()

    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    # ✅ Merge ด้วย suffix (_new, _old)
    merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

    # ✅ ระบุคอลัมน์ที่ใช้เปรียบเทียบ (ยกเว้น key และ audit fields)
    exclude_columns = [pk_column, 'order_type_id', 'create_at', 'update_at']
    compare_cols = [
        col for col in df.columns
        if col not in exclude_columns
        and f"{col}_new" in merged.columns
        and f"{col}_old" in merged.columns
    ]

    # ✅ ฟังก์ชันเปรียบเทียบอย่างปลอดภัยจาก pd.NA
    def is_different(row):
        for col in compare_cols:
            val_new = row.get(f"{col}_new")
            val_old = row.get(f"{col}_old")
            if pd.isna(val_new) and pd.isna(val_old):
                continue
            if val_new != val_old:
                return True
        return False

    # ✅ ตรวจหาความแตกต่างจริง
    df_diff = merged[merged.apply(is_different, axis=1)].copy()

    update_cols = [f"{col}_new" for col in compare_cols]
    all_cols = [pk_column] + update_cols

    df_diff_renamed = df_diff[all_cols].copy()
    df_diff_renamed.columns = [pk_column] + compare_cols  # เปลี่ยนชื่อ column ให้ตรงกับตารางจริง

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

    # ✅ Load table metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert (กรอง car_id ที่เป็น NaN)
    if not df_to_insert.empty:
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} insert rows with null car_id")
        if not df_to_insert_valid.empty:
            with target_engine.begin() as conn:
                conn.execute(metadata.insert(), df_to_insert_valid.to_dict(orient='records'))

    # ✅ Update
    if not df_diff_renamed.empty:
        with target_engine.begin() as conn:
            for record in df_diff_renamed.to_dict(orient='records'):
                stmt = pg_insert(metadata).values(**record)
                update_columns = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns
                    if c.name != pk_column
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")

# @op
# def load_order_type_data(df: pd.DataFrame):
#     table_name = 'dim_order_type'

#     # ✅ Load table metadata
#     metadata = Table(table_name, MetaData(), autoload_with=target_engine)

#     # ✅ Insert data as-is
#     if not df.empty:
#         with target_engine.begin() as conn:
#             conn.execute(metadata.insert(), df.to_dict(orient='records'))

#     print(f"✅ Inserted {len(df)} rows into {table_name}.")

@job
def dim_order_type_etl():
    load_order_type_data(clean_order_type_data(extract_order_type_data()))

# if __name__ == "__main__":
#     df_row = extract_order_type_data()
#     print("✅ Extracted logs:", df_row.shape)

#     df_clean = clean_order_type_data((df_row))
#     print("✅ Cleaned columns:", df_clean.columns)

    # output_path = "fact_check_price.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

#     load_order_type_data(df_clean)
#     print("🎉 completed! Data upserted to dim_car.")