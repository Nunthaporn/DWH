from dagster import op, job
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert
import numpy as np
import re

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


def upsert_dataframe(df, engine, table_name, pk_column):
    meta = MetaData()
    meta.reflect(bind=engine)
    table = meta.tables[table_name]

    # ✅ Fetch existing keys before upsert
    existing_df = pd.read_sql(f"SELECT {pk_column} FROM {table_name}", engine)
    existing_keys = set(existing_df[pk_column].unique())
    incoming_keys = set(df[pk_column].unique())

    new_keys = incoming_keys - existing_keys
    update_keys = incoming_keys & existing_keys

    print(f"🟢 Insert (new): {len(new_keys)}")
    print(f"📝 Update (existing): {len(update_keys)}")
    print(f"✅ Total incoming: {len(incoming_keys)}")

    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = insert(table).values(row.to_dict())
            update_dict = {c.name: stmt.excluded[c.name] for c in table.columns if c.name != pk_column}
            stmt = stmt.on_conflict_do_update(
                index_elements=[pk_column],
                set_=update_dict
            )
            conn.execute(stmt)

@op
def load_to_wh_company(df: pd.DataFrame):
    upsert_dataframe(df, target_engine, "dim_company", "company_name")
    print("✅ Upserted to dim_company successfully!")

@job
def dim_company_etl():
    load_to_wh_company(clean_company_data(extract_company_data()))

