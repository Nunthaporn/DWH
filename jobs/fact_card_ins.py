from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

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
def extract_card_ins_data():
    query = """
    SELECT cuscode, name,
           CASE 
             WHEN user_registered = '0000-00-00 00:00:00.000' THEN '2000-01-01 00:00:00'
             ELSE user_registered 
           END AS user_registered,
           idcard, card_ins, card_ins_id, card_ins_type, card_ins_life,
           card_ins_type_life, file_card_ins_life, card_ins_start,
           card_ins_exp, card_ins_life_exp, is_move_card_ins
    FROM wp_users 
    WHERE user_login NOT IN ('FINTEST-01', 'FIN-TestApp', 'Admin-VIF', 'adminmag_fin', 'FNG00-00001')
    """
    return pd.read_sql(query, source_engine)

@op
def clean_card_ins_data(df: pd.DataFrame):
    # ✅ แปลงประเภทวันที่
    df['user_registered'] = pd.to_datetime(df['user_registered'].astype(str), errors='coerce')

    # ✅ Rename columns
    df = df.rename(columns={
        "cuscode": "agent_id",
        "name": "agent_name",
        "idcard": "id_card"
    })

    # ✅ แปลงวันที่ไทยเป็น ค.ศ.
    def convert_thai_to_ad(date_str):
        try:
            if pd.isna(date_str) or date_str == '':
                return None
            day, month, year = map(int, date_str.split('-'))
            if year > 2500:
                year -= 543
            return f"{year:04d}-{month:02d}-{day:02d}"
        except:
            return None

    for col in ['card_ins_start', 'card_ins_exp']:
        df[col] = df[col].apply(convert_thai_to_ad)
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # ✅ แปลง card_ins_type_life
    df['card_ins_type_life'] = df['card_ins_type_life'].apply(lambda x: 'B' if isinstance(x, str) and 'แทน' in x else x)

    # ✅ ล้างข้อมูล
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df = df[df.count(axis=1) > 1]
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)

    # ✅ dedup agent_id
    valid_mask = df['agent_id'].astype(str).str.strip().ne('') & df['agent_id'].notna()
    df_with_id = df[valid_mask]
    df_without_id = df[~valid_mask]
    df_with_id = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates('agent_id', keep='first')
    df = pd.concat([df_with_id, df_without_id], ignore_index=True).drop(columns=['non_empty_count'])

    # ✅ เพิ่มเติม cleaning
    df.columns = df.columns.str.lower()
    df = df.replace(r'^[-\.]+$|^(?i:none|nan|undefine|undefined)$', np.nan, regex=True)
    df = df.replace(r'^\.$', np.nan, regex=True)
    df['is_move_card_ins'] = df['is_move_card_ins'].fillna(0).astype(int).astype(bool)

    # ✅ แก้ชื่อ key ก่อนโหลด
    df = df.rename(columns={'agent_id': 'card_ins_id'})

    print("\n📊 Cleaning completed")

    return df

@op
def load_card_ins_data(df: pd.DataFrame):
    table_name = 'fact_card_ins'
    pk_column = 'agent_id'

    df = df[~df[pk_column].duplicated(keep='first')].copy()

    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()
    merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

    exclude_columns = [pk_column, 'create_at', 'update_at']
    compare_cols = [
        col for col in df.columns
        if col not in exclude_columns
        and f"{col}_new" in merged.columns
        and f"{col}_old" in merged.columns
    ]

    def is_different(row):
        for col in compare_cols:
            val_new = row[f"{col}_new"]
            val_old = row[f"{col}_old"]
            if pd.isna(val_new) and pd.isna(val_old):
                continue
            if val_new != val_old:
                return True
        return False

    df_diff = merged[merged.apply(is_different, axis=1)].copy()
    df_diff_renamed = df_diff[[pk_column] + [f"{col}_new" for col in compare_cols]].copy()
    df_diff_renamed.columns = [pk_column] + compare_cols

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    if not df_to_insert.empty:
        df_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        if not df_valid.empty:
            with target_engine.begin() as conn:
                conn.execute(metadata.insert(), df_valid.to_dict(orient='records'))

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

@job
def fact_card_ins_etl():
    load_card_ins_data(clean_card_ins_data(extract_card_ins_data()))

if __name__ == "__main__":
    df_raw = extract_card_ins_data()
    print("✅ Extracted logs:", df_raw.shape)

    df_clean = clean_card_ins_data((df_raw))
    print("✅ Cleaned columns:", df_clean.columns)

    # output_path = "fact_card_ins.csv"
    # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
    # print(f"💾 Saved to {output_path}")

    output_path = "fact_card_ins.xlsx"
    df_clean.to_excel(output_path, index=False, engine='openpyxl')
    print(f"💾 Saved to {output_path}")

    # load_card_ins_data(df_clean)
    # print("🎉 completed! Data upserted to fact_card_ins.")