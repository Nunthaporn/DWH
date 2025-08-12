from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta

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
    f"postgresql+psycopg2://{target_user}:{target_password}@{target_host}:{target_port}/{target_db}",
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c statement_timeout=300000"  # 5 นาที
    },
    pool_pre_ping=True
)

@op
def extract_agent_data():

    query_main = text("""
    SELECT cuscode, name, rank,
           user_registered,
           status, fin_new_group, fin_new_mem,
           type_agent, typebuy, user_email, name_store, address, city, district,
           province, province_cur, area_cur, postcode, tel, date_active
    FROM wp_users
    WHERE user_login NOT IN ('FINTEST-01', 'FIN-TestApp', 'Admin-VIF', 'adminmag_fin', 'FNG00-00001')
        AND name NOT LIKE '%%ทดสอบ%%'
        AND name NOT LIKE '%%tes%%'
        AND cuscode NOT LIKE 'ADMIN-SALE001%%'
        AND cuscode NOT LIKE 'center_sale%%'
        AND cuscode NOT LIKE 'Client-sale%%'
        AND cuscode NOT LIKE 'Sale%%'
        AND cuscode NOT LIKE 'south%%'
        AND cuscode NOT LIKE 'mng_sale%%'
        AND cuscode NOT LIKE 'bkk%%'
        AND cuscode NOT LIKE 'east%%'
        AND cuscode NOT LIKE 'north%%'
        AND cuscode NOT LIKE 'central%%'
        AND cuscode NOT LIKE 'upc%%'
        AND cuscode NOT LIKE 'sqc_%%'
        AND cuscode NOT LIKE 'pm_%%'
        AND cuscode NOT LIKE 'Sale-Tor%%'
        AND cuscode NOT LIKE 'online%%'
        AND cuscode NOT LIKE 'Sale-Direct%%'
        AND name NOT LIKE '%%ทดสอบ%%'
        AND name NOT LIKE '%%test%%'
        AND name NOT LIKE '%%เทสระบบ%%'
        AND name NOT LIKE '%%Tes ระบบ%%'
        AND name NOT LIKE '%%ทด่ท%%'
        AND name NOT LIKE '%%ทด สอบ%%'
        AND name NOT LIKE '%%ปัญญวัฒน์ โพธิ์ศรีทอง%%'
        AND name NOT LIKE '%%เอกศิษฎ์ เจริญธันยบูรณ์%%'
        AND cuscode NOT LIKE '%%FIN-TestApp%%'
        AND cuscode NOT LIKE '%%FIN-Tester1%%'
        AND cuscode NOT LIKE '%%FIN-Tester2%%';
    """)

    df_main = pd.read_sql(query_main, source_engine)

    query_career = "SELECT cuscode, career FROM policy_register"
    df_career = pd.read_sql(query_career, source_engine)

    print("📦 df_main:", df_main.shape)
    print("📦 df_career:", df_career.shape)

    df_merged = pd.merge(df_main, df_career, on='cuscode', how='left')
    df_merged['career'] = df_merged['career'].astype(str).str.strip()

    return df_merged


@op
def clean_agent_data(df: pd.DataFrame):
    # ---------- 0) Normalize defect & agent_id ----------
    df['cuscode'] = df['cuscode'].astype(str).str.strip()
    df['status'] = df['status'].astype(str).str.strip().str.lower()

    is_defect_initial = (
        df['cuscode'].str.contains(r'-defect$', case=False, na=False) |
        df['status'].eq('defect')
    )
    base_id = df['cuscode'].str.replace(r'-defect$', '', regex=True)
    df['cuscode'] = np.where(is_defect_initial, base_id + '-defect', base_id)

    # ---------- 1) รวม region + agent_main_region ----------
    def combine_columns(a, b):
        a_str = str(a).strip() if pd.notna(a) else ''
        b_str = str(b).strip() if pd.notna(b) else ''
        if a_str == '' and b_str == '':
            return ''
        if a_str == '':
            return b_str
        if b_str == '':
            return a_str
        if a_str == b_str:
            return a_str
        return f"{a_str} + {b_str}"

    df['agent_region'] = df.apply(lambda row: combine_columns(row['fin_new_group'], row['fin_new_mem']), axis=1)

    df['agent_main_region'] = (
        df['agent_region']
          .fillna('')
          .astype(str)
          .str.replace(r'\d+', '', regex=True)
          .str.strip()
    )

    df = df[df['agent_region'] != 'TEST'].copy()
    df = df.drop(columns=['fin_new_group', 'fin_new_mem'])

    # ---------- 2) Rename columns ----------
    rename_columns = {
        "cuscode": "agent_id",
        "name": "agent_name",
        "rank": "agent_rank",
        "user_registered": "hire_date",
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
    }
    df = df.rename(columns=rename_columns)

    # defect_status หลัง rename
    is_defect_after = (
        df['agent_id'].str.contains(r'-defect$', case=False, na=False) |
        df['status'].astype(str).str.strip().str.lower().eq('defect')
    )
    df['defect_status'] = np.where(is_defect_after, 'defect', None)
    if 'status' in df.columns:
        df = df.drop(columns=['status'])

    # ---------- 3) Cleaning fields ----------
    df['is_experienced_fix'] = df['is_experienced'].apply(
        lambda x: 'เคยขาย' if str(x).strip().lower() == 'ไม่เคยขาย' else 'ไม่เคยขาย'
    )
    df = df.drop(columns=['is_experienced'])
    df = df.rename(columns={'is_experienced_fix': 'is_experienced'})

    valid_rank = [str(i) for i in range(1, 11)]
    df.loc[~df['agent_rank'].isin(valid_rank), 'agent_rank'] = np.nan

    df['agent_address_cleaned'] = df['agent_address'].apply(
        lambda addr: re.sub(r'(เลขที่|หมู่ที่|หมู่บ้าน|ซอย|ถนน)[\s\-]*', '', str(addr)).strip()
    )
    df = df.drop(columns=['agent_address'])
    df = df.rename(columns={'agent_address_cleaned': 'agent_address'})

    df['mobile_number'] = df['mobile_number'].str.replace(r'[^0-9]', '', regex=True)
    df = df.replace(r'^\s*$', pd.NA, regex=True)

    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)

    valid_mask = df['agent_id'].astype(str).str.strip().ne('') & df['agent_id'].notna()
    df_with_id = df[valid_mask]
    df_without_id = df[~valid_mask]
    df_with_id_cleaned = (
        df_with_id.sort_values('non_empty_count', ascending=False)
                  .drop_duplicates(subset='agent_id', keep='first')
    )
    df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)
    df_cleaned = df_cleaned.drop(columns=['non_empty_count'])
    df_cleaned.columns = df_cleaned.columns.str.lower()

    # hire_date → int YYYYMMDD
    df_cleaned["hire_date"] = pd.to_datetime(df_cleaned["hire_date"], errors='coerce')
    df_cleaned["hire_date"] = df_cleaned["hire_date"].dt.strftime('%Y%m%d').where(df_cleaned["hire_date"].notnull(), None)
    df_cleaned["hire_date"] = df_cleaned["hire_date"].astype('Int64')

    # --------- date_active: ทำให้เป็น datetime จริง และไม่เหลือ 'NaT' ----------
    if 'date_active' in df_cleaned.columns:
        dt = pd.to_datetime(df_cleaned["date_active"], errors='coerce')
        try:
            dt = dt.dt.tz_localize(None)
        except Exception:
            pass
        df_cleaned["date_active"] = [
            (v.to_pydatetime() if isinstance(v, pd.Timestamp) and pd.notna(v) else
             (v if isinstance(v, datetime) else None))
            for v in dt
        ]

    # อื่น ๆ
    df_cleaned["zipcode"] = df_cleaned["zipcode"].where(df_cleaned["zipcode"].str.len() == 5, np.nan)
    df_cleaned["agent_name"] = df_cleaned["agent_name"].str.lstrip()
    df_cleaned["is_experienced"] = df_cleaned["is_experienced"].apply(
        lambda x: 'no' if str(x).strip().lower() == 'ไม่เคยขาย' else 'yes'
    )

    df_cleaned["job"] = df_cleaned["job"].where(pd.notna(df_cleaned["job"]), None).replace(['NaN','nan','NULL','null'], None)
    df_cleaned["province"] = df_cleaned["province"].replace("Others", "อื่นๆ")

    def clean_email(email):
        if pd.isna(email) or email == '':
            return None
        email_str = str(email).strip()
        if re.findall(r'[ก-๙]', email_str):
            return None
        return email_str.lower() if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email_str) else None
    df_cleaned["agent_email"] = df_cleaned["agent_email"].apply(clean_email)

    def clean_agent_name(name):
        if pd.isna(name) or name == '':
            return None
        name_str = str(name).strip()
        if name_str.startswith(('ิ','ี','ึ','ื','ุ','ู','่','้','๊','๋')):
            cleaned_name = re.sub(r'^[ิีึืุู่้๊๋]+', '', name_str)
        else:
            cleaned_name = name_str
        cleaned_name = re.sub(r'[^\u0E00-\u0E7F\u0020\u0041-\u005A\u0061-\u007A]', '', cleaned_name)
        cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()
        return None if len(cleaned_name) < 2 else cleaned_name
    df_cleaned["agent_name"] = df_cleaned["agent_name"].apply(clean_agent_name)

    def clean_store_name(store_name):
        if pd.isna(store_name) or store_name == '':
            return None
        s = str(store_name).strip()
        return s if any(k in s.lower() for k in ['ร้าน','shop','store','บริษัท','company']) else None
    df_cleaned["store_name"] = df_cleaned["store_name"].apply(clean_store_name)

    def check_thai_text(text):
        if pd.isna(text) or text == '':
            return None
        cleaned_text = re.sub(r'^[\s\]\*]+', '', str(text).strip())
        for pattern in [r'^\d+$', r'^[A-Za-z\s]+$']:
            if re.match(pattern, cleaned_text):
                return None
        return cleaned_text if re.findall(r'[ก-๙]', cleaned_text) else None
    for col in ['subdistrict','district','province','current_province','current_area']:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(check_thai_text)

    def clean_zipcode(zipcode):
        if pd.isna(zipcode) or zipcode == '':
            return None
        z = str(zipcode).strip()
        return z if re.match(r'^\d{5}$', z) else None
    df_cleaned["zipcode"] = df_cleaned["zipcode"].apply(clean_zipcode)

    def clean_address(address):
        if pd.isna(address) or address == '':
            return None
        s = str(address).strip()
        s = re.sub(r'^[\u0E30-\u0E3A\u0E47-\u0E4E]+', '', s)
        s = re.sub(r'[-:.,]', '', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return s
    df_cleaned["agent_address"] = df_cleaned["agent_address"].apply(clean_address).replace(['NaN','None','NULL'], None)

    # 🔒 อย่าทำความสะอาดคอลัมน์วันที่ในลูปด้านล่าง
    date_cols = {'date_active'}
    for col in df_cleaned.columns:
        if col in {'agent_id'} | date_cols:
            continue
        if df_cleaned[col].dtype == 'object':
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: (re.sub(r'^\s+', '', str(x).strip()) or None) if pd.notna(x) and x != '' else None
            )

    print("\n📊 Cleaning completed")
    return df_cleaned


@op
def load_to_wh(df: pd.DataFrame):
    table_name = 'dim_agent'
    pk_column = 'agent_id'

    # --- sanitize รอบสุดท้าย ป้องกัน NaT/NaN ---
    if 'date_active' in df.columns:
        mask_nat_str = df['date_active'].astype(str).str.strip().str.upper().eq('NAT')
        if mask_nat_str.any():
            print(f"⚠️ Found 'NaT' as STRING in date_active: {mask_nat_str.sum()} rows (pre-coerce)")
            df.loc[mask_nat_str, 'date_active'] = None

        df['date_active'] = pd.to_datetime(df['date_active'], errors='coerce')
        try:
            df['date_active'] = df['date_active'].dt.tz_localize(None)
        except Exception:
            pass
        df['date_active'] = df['date_active'].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)

    df = df.where(pd.notnull(df), None)
    # -------------------------------------------------

    df = df[~df[pk_column].duplicated(keep='first')].copy()

    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    if df_common_new.empty or df_common_old.empty:
        merged = pd.DataFrame()
    else:
        merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

    if merged.empty:
        df_diff_renamed = pd.DataFrame()
    else:
        exclude_columns = [pk_column, 'id_contact', 'create_at', 'update_at']
        compare_cols = [
            col for col in df.columns
            if col not in exclude_columns
            and f"{col}_new" in merged.columns
            and f"{col}_old" in merged.columns
        ]
        if not compare_cols:
            df_diff_renamed = pd.DataFrame()
        else:
            def is_different(row):
                for col in compare_cols:
                    val_new = row.get(f"{col}_new")
                    val_old = row.get(f"{col}_old")
                    if pd.isna(val_new) and pd.isna(val_old):
                        continue
                    elif pd.isna(val_new) or pd.isna(val_old):
                        return True
                    elif val_new != val_old:
                        return True
                return False

            df_diff = merged[merged.apply(is_different, axis=1)].copy()
            if df_diff.empty:
                df_diff_renamed = pd.DataFrame()
            else:
                update_cols = [f"{col}_new" for col in compare_cols]
                all_cols = [pk_column] + update_cols
                missing_cols = [col for col in all_cols if col not in df_diff.columns]
                if missing_cols:
                    available_cols = [col for col in all_cols if col in df_diff.columns]
                    df_diff_renamed = df_diff[available_cols].copy()
                    available_compare_cols = [col.replace('_new', '') for col in available_cols if col != pk_column]
                    df_diff_renamed.columns = [pk_column] + available_compare_cols
                else:
                    df_diff_renamed = df_diff[all_cols].copy()
                    df_diff_renamed.columns = [pk_column] + compare_cols

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed) if 'df_diff_renamed' in locals() else 0} rows")

    metadata_table = Table(table_name, MetaData(), autoload_with=target_engine)

    def chunk_dataframe(dfx, chunk_size=500):
        for i in range(0, len(dfx), chunk_size):
            yield dfx.iloc[i:i + chunk_size]

    # ---------- helper: sanitize per-batch to dict records ----------
    def sanitize_batch(df_batch: pd.DataFrame) -> list[dict]:
        """ทำความสะอาด batch ให้ชัวร์ว่า date_active ไม่ใช่ 'NaT' string และเป็น datetime/None เท่านั้น"""
        if 'date_active' in df_batch.columns:
            mask_nat_str = df_batch['date_active'].astype(str).str.strip().str.upper().eq('NAT')
            if mask_nat_str.any():
                print(f"⚠️ Sanitize: found 'NaT' STRING {mask_nat_str.sum()} rows -> set None")
                df_batch.loc[mask_nat_str, 'date_active'] = None

            s = pd.to_datetime(df_batch['date_active'], errors='coerce')
            try:
                s = s.dt.tz_localize(None)
            except Exception:
                pass
            df_batch['date_active'] = s

        # แทน NaN -> None ทั้งกรอบ
        df_batch = df_batch.where(pd.notnull(df_batch), None)

        records = []
        for rec in df_batch.to_dict(orient="records"):
            v = rec.get("date_active", None)
            if isinstance(v, pd.Timestamp):
                rec["date_active"] = v.to_pydatetime() if pd.notna(v) else None
            elif isinstance(v, datetime):
                pass
            elif v is None:
                pass
            else:
                if isinstance(v, str) and v.strip().upper() in ("NAT", "NAN", "NULL", ""):
                    rec["date_active"] = None
                else:
                    rec["date_active"] = None
            records.append(rec)
        return records
    # ----------------------------------------------------------------

    # ✅ Upsert batch (insert ฝั่ง new)
    if not df_to_insert.empty:
        df_to_insert = df_to_insert[~df_to_insert[pk_column].duplicated(keep='first')].copy()

        with target_engine.begin() as conn:
            for batch_df in chunk_dataframe(df_to_insert):
                records = sanitize_batch(batch_df.copy())

                # debug ชี้เคสผิดปกติ (ถ้ามี)
                bad = [i for i, r in enumerate(records) if isinstance(r.get('date_active'), str)]
                if bad:
                    print(f"🧪 string date_active rows in INSERT batch: {len(bad)} -> {bad[:5]}")

                stmt = pg_insert(metadata_table).values(records)
                valid_column_names = [c.name for c in metadata_table.columns]
                update_columns = {
                    c: stmt.excluded[c]
                    for c in valid_column_names
                    if c != pk_column and c in batch_df.columns
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    # ✅ Upsert batch (update เฉพาะต่าง)
    if 'df_diff_renamed' in locals() and not df_diff_renamed.empty:
        with target_engine.begin() as conn:
            for batch_df in chunk_dataframe(df_diff_renamed):
                records = sanitize_batch(batch_df.copy())

                bad = [i for i, r in enumerate(records) if isinstance(r.get('date_active'), str)]
                if bad:
                    print(f"🧪 string date_active rows in UPDATE batch: {len(bad)} -> {bad[:5]}")

                stmt = pg_insert(metadata_table).values(records)
                valid_column_names = [c.name for c in metadata_table.columns]
                update_columns = {
                    c: stmt.excluded[c]
                    for c in valid_column_names
                    if c not in [pk_column, 'id_contact', 'create_at', 'update_at'] and c in batch_df.columns
                }
                update_columns['update_at'] = datetime.now()
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")


@op
def clean_null_values_op(df: pd.DataFrame) -> pd.DataFrame:
    # รวม 'NaT' ด้วย ป้องกัน string หลุดรอด
    return df.replace(['None', 'none', 'nan', 'NaN', 'NaT', ''], np.nan)

@job
def dim_agent_etl():
    load_to_wh(clean_agent_data(clean_null_values_op(extract_agent_data())))



# if __name__ == "__main__":
#     df_raw = extract_agent_data()
#     print("✅ Extracted logs:", df_raw.shape)

#     df_clean = clean_agent_data(df_raw)
#     print("✅ Cleaned columns:", df_clean.columns)

#     df_clean = clean_null_values_op(df_clean)

#     # output_path = "dim_agent.xlsx"
#     # df_clean.to_excel(output_path, index=False, engine='openpyxl')
#     # print(f"💾 Saved to {output_path}")

#     load_to_wh(df_clean)
#     print("🎉 Test completed! Data upserted to dim_agent.")
