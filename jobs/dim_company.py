from dagster import op, job
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert
import numpy as np
import re
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime

# ✅ โหลด .env
load_dotenv()

# DB source (MariaDB)
source_user = os.getenv('DB_USER')
source_password = os.getenv('DB_PASSWORD')
source_host = os.getenv('DB_HOST')
source_port = os.getenv('DB_PORT')
source_db = 'fininsurance'

source_engine = create_engine(
    f"mysql+pymysql://{source_user}:{source_password}@{source_host}:{source_port}/{source_db}"
)

# DB target (PostgreSQL)
target_user = os.getenv('DB_USER_test')
target_password = os.getenv('DB_PASSWORD_test')
target_host = os.getenv('DB_HOST_test')
target_port = os.getenv('DB_PORT_test')
target_db = 'fininsurance'

target_engine = create_engine(
    f"postgresql+psycopg2://{target_user}:{target_password}@{target_host}:{target_port}/{target_db}"
)

@op
def extract_company_data():
    query_main = """
        SELECT no_company,company_name,full_company,logo_path,per_ins,per_prb,api,address1,district,city,province,postcode,
        email,email_cc,email_cc2,email_cc3,email_cc4,tel1,tel2,tel3,email_follow1,email_follow2,email_non,email_non_cc,
        email_non_cc2,email_non_cc3,email_non_cc4,email_cancel,email_cancel_cc,email_cancel_cc2,email_cancel_cc3,email_cancel_cc4,
        customer_code,vendor_code
        FROM fin_car_company
    """

    query_life = """
        SELECT no_company,company_name,full_company,logo_path,address1,district,city,province,postcode,
        email,email_cc,email_cc2,email_cc3,email_cc4,tel1,tel2,tel3,email_follow1,email_follow2,email_non,email_non_cc,
        email_non_cc2,email_non_cc3,email_non_cc4,customer_code,vendor_code
        FROM fin_car_company_life
    """

    df_main = pd.read_sql(query_main, source_engine)
    df_life = pd.read_sql(query_life, source_engine)

    df_combined = pd.concat([df_main, df_life], ignore_index=True)

    # ✅ แสดงจำนวนข้อมูล
    print(f"📦 Extracted from fin_car_company: {df_main.shape[0]} rows")
    print(f"📦 Extracted from fin_car_company_life: {df_life.shape[0]} rows")
    print(f"🔗 Combined total: {df_combined.shape[0]} rows")

    return df_combined

def clean_non_printable(text):
    if isinstance(text, str):
        return re.sub(r'[^\x20-\x7Eก-๙เ-ๅ]', '', text).strip()
    return text

@op
def clean_company_data(df: pd.DataFrame):
    print(f"🧹 Start cleaning: {df.shape[0]} rows, {df.shape[1]} columns")

    rename_columns = {
        "no_company": "no_company",
        "company_name": "company_name",
        "full_company": "full_company",
        "logo_path": "logo_path",
        "per_ins": "per_ins",
        "per_prb": "per_prb",
        "api": "api",
        "address1": "address",
        "district": "subdistrict",
        "city": "district",
        "province": "province",
        "postcode": "zipcode",
        "email": "email",
        "email_cc": "email_cc1",
        "email_cc2": "email_cc2",
        "email_cc3": "email_cc3",
        "email_cc4": "email_cc4",
        "email_non": "email_non",
        "email_non_cc": "email_non_cc1",
        "email_non_cc2": "email_non_cc2",
        "email_non_cc3": "email_non_cc3",
        "email_non_cc4": "email_non_cc4",
        "email_cancel": "email_cancel",
        "email_cancel_cc": "email_cancel_cc1",
        "email_cancel_cc2": "email_cancel_cc2",
        "email_cancel_cc3": "email_cancel_cc3",
        "email_cancel_cc4": "email_cancel_cc4",
        "email_follow1": "email_follow1",
        "email_follow2": "email_follow2",   
        "tel1": "tel1",
        "tel2": "tel2",
        "tel3": "tel3",
        "customer_code": "customer_code",
        "vendor_code": "vendor_code"
    }

    df = df.rename(columns=rename_columns)

    if "api" in df.columns:
        df["api"] = pd.to_numeric(df["api"], errors="coerce").astype("Int64").astype(str)

    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)

    valid_company_mask = df['company_name'].astype(str).str.strip().ne('') & df['company_name'].notna()
    df_with_id = df[valid_company_mask]
    df_without_id = df[~valid_company_mask]

    df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='company_name', keep='first')
    df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)
    df_cleaned = df_cleaned.drop(columns=['non_empty_count'], errors='ignore')

    df_cleaned = df_cleaned.replace(to_replace=r'^\s*$|(?i:^none$)|^-$', value=np.nan, regex=True)
    df_cleaned = df_cleaned.replace(r'^\.$', np.nan, regex=True)
    df_cleaned["zipcode"] = df_cleaned["zipcode"].where(df_cleaned["zipcode"].str.len() == 5, np.nan)
    df_cleaned["per_ins"] = pd.to_numeric(df_cleaned["per_ins"], errors="coerce").astype("Int64")
    df_cleaned["per_prb"] = pd.to_numeric(df_cleaned["per_prb"], errors="coerce").astype("Int64")
    df_cleaned['api'] = df_cleaned['api'].replace('<NA>', np.nan)
    df_cleaned = df_cleaned.replace(r'^\s*$', np.nan, regex=True)

    if "district" in df_cleaned.columns:
        df_cleaned['district'] = df_cleaned['district'].str.replace('เขต', '', regex=False).str.strip()
    if "subdistrict" in df_cleaned.columns:
        df_cleaned['subdistrict'] = df_cleaned['subdistrict'].str.replace('แขวง', '', regex=False).str.strip()

    if "address" in df_cleaned.columns:
        df_cleaned["address"] = df_cleaned["address"].apply(clean_non_printable)

    # ✅ Clean tel columns
    cols_tel = ['tel1', 'tel2', 'tel3']
    for col in cols_tel:
        df_cleaned[col] = df_cleaned[col].str.replace('-', '', regex=False) \
                                         .str.replace(' ', '', regex=False) \
                                         .str.strip()

    df_cleaned.columns = df_cleaned.columns.str.lower()
    df_cleaned = df_cleaned.where(pd.notnull(df_cleaned), None)

    # ✅ Log summary
    print(f"✅ Finished cleaning: {df_cleaned.shape[0]} rows, {df_cleaned.shape[1]} columns")
    print("⚠️ Missing values summary:")
    print(df_cleaned.isna().sum())

    return df_cleaned

@op
def load_to_company(df: pd.DataFrame):
    table_name = 'dim_company'
    pk_columns = ['company_name', 'logo_path']

    # ✅ Drop duplicates ตาม composite key
    df = df.drop_duplicates(subset=pk_columns).copy()

    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    df_existing = df_existing.drop_duplicates(subset=pk_columns).copy()

    # ✅ สร้าง key เพื่อเปรียบเทียบและจัดการ insert/update
    def make_key(df):
        return df[pk_columns].astype(str).agg('|'.join, axis=1)

    df['merge_key'] = make_key(df)
    df_existing['merge_key'] = make_key(df_existing)

    new_keys = set(df['merge_key']) - set(df_existing['merge_key'])
    common_keys = set(df['merge_key']) & set(df_existing['merge_key'])

    df_to_insert = df[df['merge_key'].isin(new_keys)].copy()
    df_common_new = df[df['merge_key'].isin(common_keys)].copy()
    df_common_old = df_existing[df_existing['merge_key'].isin(common_keys)].copy()

    # ✅ 🔥 แปลง NaT → None สำหรับทุก datetime column ก่อน insert
    for col in df_to_insert.select_dtypes(include=['datetime64[ns]', 'datetimetz']).columns:
        df_to_insert[col] = df_to_insert[col].apply(lambda x: x if pd.notna(x) else None)

    merged = df_common_new.merge(df_common_old, on=pk_columns, suffixes=('_new', '_old'))

    exclude_columns = pk_columns + ['id_company', 'create_at', 'update_at']
    compare_cols = [
        col for col in df.columns
        if col not in exclude_columns
        and f"{col}_new" in merged.columns
        and f"{col}_old" in merged.columns
    ]

    def is_different(row):
        for col in compare_cols:
            if pd.isna(row[f"{col}_new"]) and pd.isna(row[f"{col}_old"]):
                continue
            if row[f"{col}_new"] != row[f"{col}_old"]:
                return True
        return False

    df_diff = merged[merged.apply(is_different, axis=1)].copy()
    update_cols = [f"{col}_new" for col in compare_cols]
    df_diff_renamed = df_diff[pk_columns + update_cols].copy()
    df_diff_renamed.columns = pk_columns + compare_cols

    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    if not df_to_insert.empty:
        with target_engine.begin() as conn:
            conn.execute(metadata.insert(), df_to_insert.drop(columns=['merge_key']).to_dict(orient='records'))

    if not df_diff_renamed.empty:
        with target_engine.begin() as conn:
            for record in df_diff_renamed.to_dict(orient='records'):
                stmt = pg_insert(metadata).values(**record)
                update_columns = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns
                    if c.name not in pk_columns + ['id_company', 'create_at', 'update_at']
                }
                # update_at ให้เป็นเวลาปัจจุบัน
                update_columns['update_at'] = datetime.now()
                stmt = stmt.on_conflict_do_update(
                    index_elements=pk_columns,
                    set_=update_columns
                )
                conn.execute(stmt)

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

@job
def dim_company_etl():
    load_to_company(clean_company_data(extract_company_data()))

if __name__ == "__main__":
    df_row = extract_company_data()
    # print("✅ Extracted logs:", df_row.shape)

    df_clean = clean_company_data((df_row))
    # print("✅ Cleaned columns:", df_clean.columns)

    # output_path = "dim_company.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_to_company(df_clean)
    print("🎉 completed! Data upserted to dim_company.")