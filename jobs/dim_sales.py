from dagster import op, job
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert
import numpy as np
import re

# ✅ โหลด env
load_dotenv()

# Source DB (MariaDB)
source_user = os.getenv('DB_USER')
source_password = os.getenv('DB_PASSWORD')
source_host = os.getenv('DB_HOST')
source_port = os.getenv('DB_PORT')
source_db = 'fininsurance'

source_engine = create_engine(
    f"mysql+pymysql://{source_user}:{source_password}@{source_host}:{source_port}/{source_db}"
)

# Target DB (PostgreSQL)
target_user = os.getenv('DB_USER_test')
target_password = os.getenv('DB_PASSWORD_test')
target_host = os.getenv('DB_HOST_test')
target_port = os.getenv('DB_PORT_test')
target_db = 'fininsurance'

target_engine = create_engine(
    f"postgresql+psycopg2://{target_user}:{target_password}@{target_host}:{target_port}/{target_db}"
)

from sqlalchemy import text

@op
def extract_sales_data():
    query_main = text("""
        SELECT cuscode, name, rank,
            CASE 
            WHEN user_registered = '0000-00-00 00:00:00.000' THEN '2000-01-01 00:00:00'
            ELSE user_registered 
            END AS user_registered,
            status, fin_new_group, fin_new_mem,
            type_agent, typebuy, user_email, name_store, address, city, district,
            province, province_cur, area_cur, postcode, tel, date_active,
            card_ins_type, file_card_ins, card_ins_type_life, file_card_ins_life
        FROM wp_users
        WHERE user_login NOT IN ('FINTEST-01', 'FIN-TestApp', 'Admin-VIF', 'adminmag_fin', 'FNG00-00001')
            AND (
                cuscode LIKE 'bkk%' OR
                cuscode LIKE '%east%' OR
                cuscode LIKE 'north%' OR
                cuscode LIKE 'central%' OR
                cuscode LIKE 'upc%' OR
                cuscode LIKE 'sqc_%' OR
                cuscode LIKE 'pm_%' OR
                cuscode LIKE 'Sale-Tor%' OR
                cuscode LIKE 'online%' OR
                cuscode LIKE 'Sale-Direct%'
            )
    """)

    df_main = pd.read_sql(query_main, source_engine)

    query_career = text("""
        SELECT cuscode, career
        FROM policy_register
    """)

    df_career = pd.read_sql(query_career, source_engine)

    df_merged = pd.merge(df_main, df_career, on='cuscode', how='left')
    return df_merged


@op
def clean_sales_data(df: pd.DataFrame):
    df['agent_region'] = df.apply(
        lambda row: f"{row['fin_new_group']} + {row['fin_new_mem']}" if pd.notna(row['fin_new_group']) and pd.notna(row['fin_new_mem']) else row['fin_new_group'] or row['fin_new_mem'],
        axis=1
    )
    df = df[df['agent_region'] != 'TEST']
    df = df.drop(columns=['fin_new_group', 'fin_new_mem'], errors='ignore')

    df['date_active'] = pd.to_datetime(df['date_active'], errors='coerce')
    now = pd.Timestamp.now()
    one_month_ago = now - pd.DateOffset(months=1)

    def check_condition(row):
        if row['status'] == 'defect':
            return 'inactive'
        elif pd.notnull(row['date_active']) and row['date_active'] < one_month_ago:
            return 'inactive'
        else:
            return 'active'
    df['status_agent'] = df.apply(check_condition, axis=1)
    df = df.drop(columns=['status','date_active'])

    df = df.rename(columns=rename_columns)

    # ✅ หลัง rename แล้ว ใช้ agent_id ในการสร้าง defect_status
    df['defect_status'] = np.where(df['agent_id'].str.contains('-defect', na=False), 'defect', None)
    df['cuscode'] = df['cuscode'].str.replace('-defect', '', regex=False)

    rename_columns = {
        "cuscode": "agent_id",
        "name": "agent_name",
        "rank": "agent_rank",
        "user_registered": "hire_date",
        "status_agent": "status_agent",
        "type_agent": "type_agent",
        "typebuy": "is_experienced",
        "user_email": "agent_email",
        "name_store": "store_name",
        "address": "agent_address",
        "city": "subdistrict",
        "district": "district",
        "province": "province",
        "province_cur": "current_province",
        "area_cur": "current_area",
        "postcode": "zipcode",
        "tel": "mobile_number",
        "career": "job",
        "agent_region": "agent_region",
        "defect_status": "defect_status",
        "card_ins_type": "card_ins_type",
        "file_card_ins": "file_card_ins",
        "card_ins_type_life": "card_ins_type_life",
        "file_card_ins_life": "file_card_ins_life"
    }

    df = df.rename(columns=rename_columns)

    df['card_ins_type_life'] = df['card_ins_type_life'].apply(
        lambda x: 'B' if isinstance(x, str) and 'แทน' in x else x
    )
    df['is_experienced_fix'] = df['is_experienced'].apply(lambda x: 'เคยขาย' if str(x).strip().lower() == 'ไม่เคยขาย' else 'ไม่เคยขาย')
    df = df.drop(columns=['is_experienced'])
    df.rename(columns={'is_experienced_fix': 'is_experienced'}, inplace=True)

    valid_types = ['BUY', 'SELL', 'SHARE']
    df.loc[~df['type_agent'].isin(valid_types), 'type_agent'] = np.nan

    valid_rank = [str(i) for i in range(1, 11)]
    df.loc[~df['agent_rank'].isin(valid_rank), 'agent_rank'] = np.nan

    def clean_address(addr):
        if pd.isna(addr):
            return ''
        addr = re.sub(r'(เลขที่|หมู่ที่|หมู่บ้าน|ซอย|ถนน)[\s\-]* ', '', addr, flags=re.IGNORECASE)
        addr = re.sub(r'\s*-\s*', '', addr)
        addr = re.sub(r'\s+', ' ', addr)
        return addr.strip()

    df['agent_address'] = df['agent_address'].apply(clean_address)
    df['mobile_number'] = df['mobile_number'].str.replace(r'[^0-9]', '', regex=True)

    df['is_experienced'] = df['is_experienced'].apply(lambda x: 'yes' if str(x).strip().lower() == 'no' else 'no')
    df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce')
    df['hire_date'] = df['hire_date'].astype('int64') // 10**9
    df['hire_date'] = df['hire_date'].where(df['hire_date'].notnull(), None)
    df["zipcode"] = df["zipcode"].where(df["zipcode"].str.len() == 5, np.nan)

    df.columns = df.columns.str.lower()
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.where(pd.notnull(df), None)

    return df

def upsert_dataframe(df, engine, table_name, pk_column):
    meta = MetaData()
    meta.reflect(bind=engine)
    table = meta.tables[table_name]

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
def load_to_wh_sales(df: pd.DataFrame):
    upsert_dataframe(df, target_engine, "dim_sales", "agent_id")
    print("✅ Upserted to dim_sales successfully!")

@job
def dim_sales_etl():
    load_to_wh_sales(clean_sales_data(extract_sales_data()))


if __name__ == "__main__":
    df_raw = extract_sales_data()
    print("✅ Extracted:", df_raw.shape)

    df_clean = clean_sales_data(df_raw)
    print("✅ Cleaned columns:", df_clean.columns)

    upsert_dataframe(df_clean, target_engine, "dim_sales", "agent_id")
    print("🎉 Test completed! Data upserted to dim_sales.")
