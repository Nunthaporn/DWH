from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from fuzzywuzzy import fuzz

# ✅ โหลด .env
load_dotenv()

# ✅ DB Connections
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)

target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

# ✅ Extract
@op
def extract_customer_data():
    query_pay = """
        SELECT quo_num, address, province, amphoe, district, zipcode, datestart
        FROM fin_system_pay
        WHERE datestart >= '2025-01-01' AND datestart < '2025-08-10'
    """
    df_pay = pd.read_sql(query_pay, source_engine)

    query_plan = """
        SELECT quo_num, idcard, title, name, lastname, birthDate, career, gender, tel, email, datestart
        FROM fin_system_select_plan
        WHERE datestart >= '2025-01-01' AND datestart < '2025-08-10'
    """
    df_plan = pd.read_sql(query_plan, source_engine)

    df_merged = pd.merge(df_pay, df_plan, on='quo_num', how='right')
    df_merged = df_merged.replace(r'NaN', np.nan, regex=True)
    return df_merged

@op
def clean_customer_data(df: pd.DataFrame):
    df = df.drop(columns=['datestart_x', 'datestart_y'], errors='ignore')

    df['full_name'] = df.apply(
        lambda row: row['name'] if str(row['name']).strip() == str(row['lastname']).strip()
        else f"{str(row['name']).strip()} {str(row['lastname']).strip()}",
        axis=1
    )

    df = df.drop(columns=['name', 'lastname', 'quo_num'])

    df['birthDate'] = pd.to_datetime(df['birthDate'], errors='coerce')
    df['birthDate'] = df['birthDate'].where(df['birthDate'].notna(), None)

    df['age'] = df['birthDate'].apply(
        lambda x: (
            date.today().year - x.year - ((date.today().month, date.today().day) < (x.month, x.day))
            if isinstance(x, pd.Timestamp) else pd.NA
        )
    ).astype('Int64')

    df = df.rename(columns={
        'idcard': 'customer_card',
        'title': 'title',
        'full_name': 'customer_name',
        'birthDate': 'customer_dob',
        'gender': 'customer_gender',
        'tel': 'customer_telnumber',
        'email': 'customer_email',
        'address': 'address',
        'province': 'province',
        'amphoe': 'district',
        'district': 'subdistrict',
        'zipcode': 'zipcode',
        'career': 'job'
    })

    df['customer_dob'] = df['customer_dob'].apply(lambda x: x if isinstance(x, pd.Timestamp) else None)
    df['customer_dob'] = df['customer_dob'].astype(object)
    df['customer_gender'] = df['customer_gender'].astype(str).str.strip().str.lower()
    df['customer_gender'] = df['customer_gender'].replace({
        'M': 'Male',
        'ชาย': 'Male',
        'F': 'Female',
        'หญิง': 'Female',
        'อื่นๆ': 'Other',
        'O': 'Other'
    })

    valid_genders = ['Male', 'Female', 'Other']
    df['customer_gender'] = df['customer_gender'].where(df['customer_gender'].isin(valid_genders), None)

    df['customer_name'] = df['customer_name'].str.replace(r'\s*None$', '', regex=True)
    df['customer_telnumber'] = df['customer_telnumber'].str.replace('-', '', regex=False)
    df['address'] = df['address'].str.replace('-', '', regex=False).str.strip()
    df['address'] = df['address'].str.replace(r'^[ุูึืิ]+', '', regex=True)
    df['address'] = df['address'].str.replace(r'\([^)]*\)', '', regex=True)
    df['address'] = df['address'].str.lstrip(':').replace('/', pd.NA).replace('', pd.NA)
    df['customer_email'] = df['customer_email'].str.replace('_', '', regex=False)
    df['title'] = df['title'].str.replace('‘นาย', 'นาย', regex=False).str.strip()

    test_names = [
        'ทดสอบ', 'ทดสอบ ', 'ทดสอบ จากฟิน', 'ทดสอบ พ.ร.บ.', 'ทดสอบ06',
        'ทดสอบระบบ ประกัน+พ.ร.บ.', 'ลูกค้า ทดสอบ', 'ทดสอบ เช็คเบี้ย',
        'ทดสอบพ.ร.บ. งานคีย์มือ', 'ทดสอบ ระบบ', 'ทดสอบคีย์มือ ธนชาตผู้ขับขี่',
        'ทดสอบ04', 'test', 'test2', 'test tes', 'test ระบบ', 'Tes ระบบ'
    ]
    df = df[~df['customer_name'].isin(test_names)]

    def clean_tel(val):
        if pd.isnull(val) or val.strip() == "":
            return None
        digits = re.sub(r'\D', '', val)
        return digits if digits else None

    df['customer_telnumber'] = df['customer_telnumber'].apply(clean_tel)

    # ✅ ลบ fuzzy duplicates ใน customer_card
    def remove_fuzzy_duplicates(df, threshold=90):
        df['customer_card_clean'] = df['customer_card'].astype(str).str.strip()
        df['customer_name_clean'] = df['customer_name'].astype(str).str.strip().str.lower()

        keep_rows = []
        for card, group in df.groupby('customer_card_clean'):
            names = group['customer_name'].dropna().unique().tolist()
            seen = set()
            for i, name1 in enumerate(names):
                if name1 in seen:
                    continue
                for name2 in names[i+1:]:
                    score = fuzz.ratio(name1.strip().lower(), name2.strip().lower())
                    if score >= threshold:
                        seen.add(name2)
            keep_rows.extend(group[~group['customer_name'].isin(seen)].index.tolist())

        return df.loc[keep_rows].copy()

    df = remove_fuzzy_duplicates(df)
    df = df.drop(columns=['customer_card_clean', 'customer_name_clean'], errors='ignore')
    df = df.replace(r'NaN', np.nan, regex=True)
    return df

@op
def load_customer_data(df: pd.DataFrame):
    table_name = 'dim_customer'
    pk_columns = ['customer_card', 'customer_name']  # Composite key

    # ✅ ตรวจสอบว่าตารางมี column 'quotation_num' หรือไม่ — ถ้าไม่มีก็สร้าง
    with target_engine.connect() as conn:
        inspector = inspect(conn)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        if 'quotation_num' not in columns:
            print("➕ Adding missing column 'quotation_num' to dim_car")
            conn.execute(f'ALTER TABLE {table_name} ADD COLUMN quotation_num VARCHAR')

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

    exclude_columns = pk_columns + ['customer_sk', 'create_at', 'update_at']
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
                    if c.name not in pk_columns
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=pk_columns,
                    set_=update_columns
                )
                conn.execute(stmt)

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

# ✅ Dagster job
@job
def dim_customer_etl():
    load_customer_data(clean_customer_data(extract_customer_data()))

if __name__ == "__main__":
    df_raw = extract_customer_data()
    print("✅ Extracted logs:", df_raw.shape)

    df_clean = clean_customer_data((df_raw))
    print("✅ Cleaned columns:", df_clean.columns)

    # print(df_clean.head(10))

    # output_path = "dim_customer.csv"
    # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
    # print(f"💾 Saved to {output_path}")

    output_path = "dim_customer.xlsx"
    df_clean.to_excel(output_path, index=False, engine='openpyxl')
    print(f"💾 Saved to {output_path}")

    # load_customer_data(df_clean)
    # print("🎉 Test completed! Data upserted to dim_customer.")