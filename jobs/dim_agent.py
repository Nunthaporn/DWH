from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ✅ โหลด .env
load_dotenv()

# ✅ DB source (MariaDB)
source_user = os.getenv('DB_USER')
source_password = os.getenv('DB_PASSWORD')
source_host = os.getenv('DB_HOST')
source_port = os.getenv('DB_PORT')
source_db = 'fininsurance'

source_engine = create_engine(
    f"mysql+pymysql://{source_user}:{source_password}@{source_host}:{source_port}/{source_db}"
)

# ✅ DB target (PostgreSQL)
target_user = os.getenv('DB_USER_test')
target_password = os.getenv('DB_PASSWORD_test')
target_host = os.getenv('DB_HOST_test')
target_port = os.getenv('DB_PORT_test')
target_db = 'fininsurance'

target_engine = create_engine(
    f"postgresql+psycopg2://{target_user}:{target_password}@{target_host}:{target_port}/{target_db}"
)

@op
def extract_agent_data():
    query_main = r"""
    SELECT cuscode, name, rank,
           CASE 
           WHEN user_registered = '0000-00-00 00:00:00.000' THEN '2000-01-01 00:00:00'
             ELSE user_registered 
           END AS user_registered,
           status, fin_new_group, fin_new_mem,
           type_agent, typebuy, user_email, name_store, address, city, district,
           province, province_cur, area_cur, postcode, tel,date_active,card_ins_type,file_card_ins,
           card_ins_type_life,file_card_ins_life
    FROM wp_users 
    WHERE user_login NOT IN ('FINTEST-01', 'FIN-TestApp', 'Admin-VIF', 'adminmag_fin', 'FNG00-00001')
      AND name NOT LIKE '%%ทดสอบ%%'
      AND name NOT LIKE '%%tes%%'
      AND cuscode NOT LIKE 'bkk%%'
      AND cuscode NOT LIKE '%%east%%'
      AND cuscode NOT LIKE 'north%%'
      AND cuscode NOT LIKE 'central%%'
      AND cuscode NOT LIKE 'upc%%'
      AND cuscode NOT LIKE 'sqc_%%'
      AND cuscode NOT LIKE 'pm_%%'
      AND cuscode NOT LIKE 'Sale-Tor%%'
      AND cuscode NOT LIKE 'online%%'
      AND cuscode NOT LIKE 'Sale-Direct%%'
    """


    df_main = pd.read_sql(query_main, source_engine)

    query_career = "SELECT cuscode, career FROM policy_register"
    df_career = pd.read_sql(query_career, source_engine)

    df_merged = pd.merge(df_main, df_career, on='cuscode', how='left')

    return df_merged

@op
def clean_agent_data(df: pd.DataFrame):
    # Combine region columns
    def combine_columns(a, b):
        a_str = str(a).strip() if pd.notna(a) else ''
        b_str = str(b).strip() if pd.notna(b) else ''
        if a_str == '' and b_str == '':
            return ''
        elif a_str == '':
            return b_str
        elif b_str == '':
            return a_str
        elif a_str == b_str:
            return a_str
        else:
            return f"{a_str} + {b_str}"

    df['agent_region'] = df.apply(lambda row: combine_columns(row['fin_new_group'], row['fin_new_mem']), axis=1)
        # ✅ กรอง row ที่ agent_region = 'TEST' ออก
    df = df[df['agent_region'] != 'TEST']
    df = df.drop(columns=['fin_new_group', 'fin_new_mem'])

    # Clean date_active and status_agent
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
    df = df.drop(columns=['status', 'date_active'])

    df['defect_status'] = np.where(df['cuscode'].str.contains('-defect', na=False), 'defect', None)

    # Rename columns
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
    df['defect_status'] = np.where(df['agent_id'].str.contains('-defect', na=False), 'defect', None)

    # Clean fields
    df['card_ins_type_life'] = df['card_ins_type_life'].apply(lambda x: 'B' if isinstance(x, str) and 'แทน' in x else x)
    df['is_experienced_fix'] = df['is_experienced'].apply(lambda x: 'เคยขาย' if str(x).strip().lower() == 'ไม่เคยขาย' else 'ไม่เคยขาย')
    df = df.drop(columns=['is_experienced'])
    df.rename(columns={'is_experienced_fix': 'is_experienced'}, inplace=True)

    valid_rank = [str(i) for i in range(1, 11)]
    df.loc[~df['agent_rank'].isin(valid_rank), 'agent_rank'] = np.nan

    df['agent_address_cleaned'] = df['agent_address'].apply(lambda addr: re.sub(r'(เลขที่|หมู่ที่|หมู่บ้าน|ซอย|ถนน)[\s\-]*', '', str(addr)).strip())
    df = df.drop(columns=['agent_address'])
    df.rename(columns={'agent_address_cleaned': 'agent_address'}, inplace=True)

    df['mobile_number'] = df['mobile_number'].str.replace(r'[^0-9]', '', regex=True)
    df = df.replace(r'^\s*$', pd.NA, regex=True)

    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)

    valid_mask = df['agent_id'].astype(str).str.strip().ne('') & df['agent_id'].notna()
    df_with_id = df[valid_mask]
    df_without_id = df[~valid_mask]
    df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='agent_id', keep='first')
    df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)
    df_cleaned = df_cleaned.drop(columns=['non_empty_count'])
    df_cleaned.columns = df_cleaned.columns.str.lower()

    df_cleaned["hire_date"] = pd.to_datetime(df_cleaned["hire_date"], errors='coerce')
    df_cleaned["hire_date"] = df_cleaned["hire_date"].dt.strftime('%Y%m%d')
    df_cleaned["hire_date"] = df_cleaned["hire_date"].where(df_cleaned["hire_date"].notnull(), None)
    df_cleaned["hire_date"] = df_cleaned["hire_date"].astype('Int64')

    df_cleaned["zipcode"] = df_cleaned["zipcode"].where(df_cleaned["zipcode"].str.len() == 5, np.nan)
    df_cleaned["agent_name"] = df_cleaned["agent_name"].str.lstrip()
    df_cleaned["is_experienced"] = df_cleaned["is_experienced"].apply(lambda x: 'no' if str(x).strip().lower() == 'ไม่เคยขาย' else 'yes')

    return df_cleaned

@op
def load_to_wh(df: pd.DataFrame):
    table_name = 'dim_agent'
    pk_column = 'agent_id'
    enum_columns = {
        'agent_rank': 'rank_enum',
        'status_agent': 'status_enum',
        'is_experienced': 'yes_no_enum'
    }

    df.columns = df.columns.str.strip()

    # ✅ Fetch existing keys before upsert
    existing_df = pd.read_sql(f"SELECT {pk_column} FROM {table_name}", target_engine)
    existing_keys = set(existing_df[pk_column].unique())
    incoming_keys = set(df[pk_column].unique())

    new_keys = incoming_keys - existing_keys
    update_keys = incoming_keys & existing_keys

    print(f"🟢 Insert (new): {len(new_keys)}")
    print(f"📝 Update (existing): {len(update_keys)}")
    print(f"✅ Total incoming: {len(incoming_keys)}")

    # ทำ temp table
    temp_table = f"{table_name}_temp"
    df.to_sql(temp_table, target_engine, if_exists='replace', index=False)

    columns = df.columns.tolist()
    select_expr = [f"{col}::{enum_columns[col]}" if col in enum_columns else col for col in columns]
    select_columns_str = ', '.join(select_expr)
    columns_str = ', '.join(columns)
    update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != pk_column])

    upsert_sql = f"""
        INSERT INTO {table_name} ({columns_str})
        SELECT {select_columns_str} FROM {temp_table}
        ON CONFLICT ({pk_column})
        DO UPDATE SET {update_str};

        DROP TABLE {temp_table};
    """

    with target_engine.begin() as conn:
        conn.execute(text(upsert_sql))

    print(f"✅ Upserted to {table_name} successfully!")
    
@job
def dim_agent_etl():
    load_to_wh(clean_agent_data(extract_agent_data()))