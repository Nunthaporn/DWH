from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.dialects.postgresql import insert

# ✅ โหลด env
load_dotenv()

# ✅ DB Connections
# Source DB (MariaDB)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)

# Target DB (PostgreSQL)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

# ✅ Extract
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
    df_main.replace(r'NaN', np.nan, regex=True, inplace=True)
    query_career = text("SELECT cuscode, career FROM policy_register")
    df_career = pd.read_sql(query_career, source_engine)

    return pd.merge(df_main, df_career, on='cuscode', how='left')

# ✅ Transform
@op
def clean_sales_data(df: pd.DataFrame):
    df['agent_region'] = df.apply(
        lambda row: f"{row['fin_new_group']} + {row['fin_new_mem']}"
        if pd.notna(row['fin_new_group']) and pd.notna(row['fin_new_mem'])
        else row['fin_new_group'] or row['fin_new_mem'],
        axis=1
    )
    df = df[df['agent_region'] != 'TEST']
    df = df.drop(columns=['fin_new_group', 'fin_new_mem'], errors='ignore')

    df['date_active'] = pd.to_datetime(df['date_active'], errors='coerce')
    one_month_ago = pd.Timestamp.now() - pd.DateOffset(months=1)

    def check_status(row):
        if row['status'] == 'defect':
            return 'inactive'
        elif pd.notnull(row['date_active']) and row['date_active'] < one_month_ago:
            return 'inactive'
        return 'active'
    df['status_agent'] = df.apply(check_status, axis=1)
    df = df.drop(columns=['status', 'date_active'])

    df['defect_status'] = np.where(df['cuscode'].str.contains('-defect', na=False), 'defect', None)
    df['cuscode'] = df['cuscode'].str.replace('-defect', '', regex=False)

    rename_map = {
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
    df.rename(columns=rename_map, inplace=True)

    df['card_ins_type_life'] = df['card_ins_type_life'].apply(
        lambda x: 'B' if isinstance(x, str) and 'แทน' in x else x
    )

    df['is_experienced'] = df['is_experienced'].apply(lambda x: 'เคยขาย' if str(x).strip().lower() == 'ไม่เคยขาย' else 'ไม่เคยขาย')
    df['is_experienced'] = df['is_experienced'].apply(lambda x: 'yes' if str(x).strip().lower() == 'no' else 'no')

    df.loc[~df['type_agent'].isin(['BUY', 'SELL', 'SHARE']), 'type_agent'] = np.nan
    df.loc[~df['agent_rank'].astype(str).isin([str(i) for i in range(1, 11)]), 'agent_rank'] = np.nan

    def clean_address(addr):
        if pd.isna(addr):
            return ''
        
        addr = str(addr).strip()

        addr = re.sub(r':undefined\s*::::', '', addr, flags=re.IGNORECASE)
        addr = re.sub(r':undefined', '', addr, flags=re.IGNORECASE)
        addr = re.sub(r':\s*-', '', addr)
        addr = re.sub(r':-', '', addr)
        addr = re.sub(r':', ' ', addr)
        addr = re.sub(r'\s*-\s*', ' ', addr)
        addr = re.sub(r'\s+', ' ', addr)
        return addr.strip()

    df['agent_address'] = df['agent_address'].apply(clean_address)
    df['mobile_number'] = df['mobile_number'].str.replace(r'[^0-9]', '', regex=True)

    df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce')
    df['hire_date'] = df['hire_date'].dt.strftime('%Y%m%d').astype('Int64')
    df['hire_date'] = df['hire_date'].where(df['hire_date'].notnull(), None)

    df["zipcode"] = df["zipcode"].where(df["zipcode"].str.len() == 5, np.nan)

    df.columns = df.columns.str.lower()
    df.replace(r'NaN', np.nan, regex=True, inplace=True)
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df = df.where(pd.notnull(df), None)

    return df

# ✅ Load
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
            stmt = stmt.on_conflict_do_update(index_elements=[pk_column], set_=update_dict)
            conn.execute(stmt)

@op
def load_to_wh_sales(df: pd.DataFrame):
    upsert_dataframe(df, target_engine, "dim_sales", "agent_id")
    print("✅ Upserted to dim_sales successfully!")

# ✅ Dagster Job
@job
def dim_sales_etl():
    load_to_wh_sales(clean_sales_data(extract_sales_data()))

# ✅ Local Execution
if __name__ == "__main__":
    df_raw = extract_sales_data()
    print("✅ Extracted:", df_raw.shape)

    df_clean = clean_sales_data(df_raw)
    print("✅ Cleaned columns:", df_clean.columns)

    # output_path = "cleaned_dim_sales.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    # ✅ Test upsert manually (optional)
    upsert_dataframe(df_clean, target_engine, "dim_sales", "agent_id")
    print("🎉 Test completed! Data upserted to dim_sales.")
