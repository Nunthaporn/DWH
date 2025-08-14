from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta

# =======================
# CONFIG
# =======================
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

# ✅ IDs ที่ต้องคงไว้เสมอ แม้จะเข้ากฎ “ทดสอบ”
WHITELIST = {"WEB-T2R", "FTR22-9999"}
PROBE_IDS = {"WEB-T2R", "FTR22-9999"}

# ✅ user_login blacklist
LOGIN_BLACKLIST = {'FINTEST-01', 'FIN-TestApp', 'Admin-VIF', 'adminmag_fin', 'FNG00-00001'}

# =======================
# HELPERS
# =======================
def timestamp_str():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def filter_and_log_drops(df_raw: pd.DataFrame):
    """
    คัดกรอง ‘ทดสอบ/test’ อย่างระมัดระวัง + keep whitelist เสมอ
    คืน (df_keep, df_drop_with_reason)
    """
    df = df_raw.copy()

    s_cus   = df['cuscode'].astype(str)
    s_login = df['user_login'].astype(str)
    s_name  = df['name'].astype(str)

    cus_up = s_cus.str.upper().str.strip()
    keep_whitelist = cus_up.isin({x.upper() for x in WHITELIST})

    # 1) login blacklist
    drop_login_blacklist = s_login.isin(LOGIN_BLACKLIST)

    # 2) คำไทย "ทดสอบ/เทสระบบ/ทด สอบ/ทด่ท" ฯลฯ
    #   - ผ่อนกฎ: ถ้า cuscode เป็นรูปแบบงานจริง (เช่น FTRxx-xxxx) ให้ไม่ตัด
    real_code = s_cus.str.match(r'^(FTR\d{2}-\d{4}|WEB-[A-Z0-9]+)$', na=False)
    name_th_suspect = s_name.str.contains(r'(ทด\s*สอบ|ทด่ท|เทสระบบ)', regex=True, na=False)
    drop_name_test_th = name_th_suspect & ~real_code

    # 3) อังกฤษ “test” ใช้ lookaround ปลอด warning + ไม่ตัดถ้าเป็นรหัสงานจริง
    drop_name_test_en = s_name.str.contains(r'(?i)(?<![a-z])test(?![a-z])', regex=True, na=False) & ~real_code

    # 4) คำใน cuscode ที่ blacklist เฉพาะแอคเคานท์ทดสอบของระบบ
    drop_cus_blacklist = s_cus.str.contains(r'(?i)FIN-(TestApp|Tester1|Tester2)', regex=True, na=False)

    # รวม reason แบบระบุรายละเอียด
    reasons = []
    for i in range(len(df)):
        r = []
        if drop_login_blacklist.iat[i]: r.append('drop_login_blacklist')
        if drop_name_test_th.iat[i]:   r.append('drop_name_test_th')
        if drop_name_test_en.iat[i]:   r.append('drop_name_test_en')
        if drop_cus_blacklist.iat[i]:  r.append('drop_cus_blacklist')
        reasons.append(",".join(r) if r else "")

    df['drop_reason'] = reasons

    # keep สำหรับ whitelist เสมอ
    to_drop = (df['drop_reason'] != "") & (~keep_whitelist)
    df_keep = df[~to_drop].copy()
    df_drop = df[to_drop].copy()

    # LOG
    print(f"TOTAL RAW: {len(df)}")
    print(f"KEEP: {len(df_keep)}  | DROP: {len(df_drop)}")
    if not df_drop.empty:
        top = (df_drop['drop_reason'].value_counts()).head(10)
        print("TOP DROP REASONS:\n", top)

    # dump dropped rows for audit
    if not df_drop.empty:
        out = f"dim_agent_dropped_{timestamp_str()}.xlsx"
        try:
            df_drop.to_excel(out, index=False, engine='openpyxl')
            print(f"💾 Saved dropped rows -> {out} ({len(df_drop)} rows)")
        except Exception as e:
            print(f"⚠️ Save dropped failed: {e}")

    # PROBE
    for pid in PROBE_IDS:
        raw_cnt  = (cus_up == pid.upper()).sum()
        keep_cnt = (df_keep['cuscode'].astype(str).str.upper() == pid.upper()).sum()
        drop_cnt = (df_drop['cuscode'].astype(str).str.upper() == pid.upper()).sum()
        print(f"--- PROBE {pid} ---")
        print(f"raw: {raw_cnt} keep: {keep_cnt} drop: {drop_cnt}")
        if drop_cnt:
            print(df_drop.loc[df_drop['cuscode'].astype(str).str.upper() == pid.upper(),
                              ['cuscode','user_login','name','fin_new_group','fin_new_mem','drop_reason']].head())

    return df_keep, df_drop


# =======================
# EXTRACT
# =======================
@op
def extract_agent_data():
    query_main = text("""
        SELECT
            cuscode, user_login, name, rank,
            user_registered,
            status, fin_new_group, fin_new_mem,
            type_agent, typebuy, user_email, name_store, address, city, district,
            province, province_cur, area_cur, postcode, tel, date_active
        FROM wp_users
    """)
    df_main = pd.read_sql(query_main, source_engine)

    query_career = "SELECT cuscode, career FROM policy_register"
    df_career = pd.read_sql(query_career, source_engine)

    print("📦 df_main:", df_main.shape)
    print("📦 df_career:", df_career.shape)

    # รวม career (ซ้ายเก็บครบ)
    df_merged = pd.merge(df_main, df_career, on='cuscode', how='left')
    df_merged['career'] = df_merged['career'].astype(str).str.strip()

    # DEBUG RAW
    print("RAW COUNT:", len(df_merged))
    print("RAW UNIQUE cuscode:", df_merged['cuscode'].nunique())

    for pid in PROBE_IDS:
        print(f"🔎 RAW has {pid}:", (df_merged['cuscode'].astype(str).str.upper() == pid.upper()).sum())

    # คัดกรอง + log
    df_filtered, _ = filter_and_log_drops(df_merged)

    return df_filtered


# =======================
# CLEAN
# =======================
@op
def clean_agent_data(df: pd.DataFrame):
    # ---------- 0) Normalize defect & cuscode ----------
    df['cuscode'] = df['cuscode'].astype(str).str.strip()
    had_suffix = df['cuscode'].str.contains(r'-defect$', case=False, na=False)
    base_id = df['cuscode'].str.replace(r'-defect$', '', regex=True)
    df['cuscode'] = np.where(had_suffix, base_id + '-defect', base_id)

    # defect_status แยกคอลัมน์
    s_status = df['status'].astype(str).str.strip().str.lower()
    is_defect_after = (
        df['cuscode'].str.contains(r'-defect$', case=False, na=False) |
        s_status.eq('defect')
    )
    df['defect_status'] = np.where(is_defect_after, 'defect', None)

    # ---------- 1) รวม region ----------
    def combine_columns(a, b):
        a_str = str(a).strip() if pd.notna(a) else ''
        b_str = str(b).strip() if pd.notna(b) else ''
        if not a_str and not b_str: return ''
        if not a_str: return b_str
        if not b_str: return a_str
        return a_str if a_str == b_str else f"{a_str} + {b_str}"

    df['agent_region'] = df.apply(lambda r: combine_columns(r['fin_new_group'], r['fin_new_mem']), axis=1)

    # ---------- 1.1 กรอง TEST แบบ exact (หลังจากผ่าน filter หลักแล้วปกติจะเหลือไม่เยอะ)
    g = df['fin_new_group'].astype(str).str.strip().str.upper()
    m = df['fin_new_mem'  ].astype(str).str.strip().str.upper()
    mask_test_exact = g.eq('TEST') & m.eq('TEST')
    mask_keep_whitelist = df['cuscode'].astype(str).str.upper().isin({x.upper() for x in WHITELIST})

    before = len(df)
    df = df[~(mask_test_exact & ~mask_keep_whitelist)].copy()
    print(f"AFTER TEST FILTER (in clean): rows = {len(df)} | dropped: {before - len(df)}")

    # agent_main_region
    df['agent_main_region'] = (
        df['agent_region'].fillna('').astype(str)
          .str.replace(r'\d+', '', regex=True).str.strip()
    )

    df.drop(columns=['fin_new_group', 'fin_new_mem'], inplace=True, errors='ignore')

    # ---------- 2) Rename columns ----------
    df = df.rename(columns={
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
    })

    # probe
    for pid in PROBE_IDS:
        print(f"🔎 CLEAN has {pid}:", (df['agent_id'].astype(str).str.upper() == pid.upper()).sum())

    # ---------- 3) Cleaning ----------
    if 'status' in df.columns:
        s_status2 = df['status'].astype(str).str.strip().str.lower()
        df['defect_status'] = np.where(
            (df['defect_status'] == 'defect') | s_status2.eq('defect'),
            'defect', None
        )
        df.drop(columns=['status'], inplace=True)

    # is_experienced ปรับค่า
    df['is_experienced_fix'] = df['is_experienced'].apply(
        lambda x: 'เคยขาย' if str(x).strip().lower() == 'ไม่เคยขาย' else 'ไม่เคยขาย'
    )
    df.drop(columns=['is_experienced'], inplace=True)
    df.rename(columns={'is_experienced_fix': 'is_experienced'}, inplace=True)

    # rank
    valid_rank = [str(i) for i in range(1, 11)]
    df.loc[~df['agent_rank'].isin(valid_rank), 'agent_rank'] = np.nan

    # address
    df['agent_address_cleaned'] = df['agent_address'].apply(
        lambda addr: re.sub(r'(เลขที่|หมู่ที่|หมู่บ้าน|ซอย|ถนน)[\s\-]*', '', str(addr)).strip()
    )
    df.drop(columns=['agent_address'], inplace=True)
    df.rename(columns={'agent_address_cleaned': 'agent_address'}, inplace=True)

    # mobile
    df['mobile_number'] = df['mobile_number'].astype(str).str.replace(r'[^0-9]', '', regex=True)
    df = df.replace(r'^\s*$', pd.NA, regex=True)

    # (ออปชัน) ปิด dedup ตาม agent_id (เปิดถ้าต้องการ)
    DEDUP_BY_AGENT = False
    if DEDUP_BY_AGENT:
        df_temp = df.replace(r'^\s*$', np.nan, regex=True)
        df['non_empty_count'] = df_temp.notnull().sum(axis=1)
        mask_valid = df['agent_id'].astype(str).str.strip().ne('') & df['agent_id'].notna()
        df_with_id = df[mask_valid]
        df_without_id = df[~mask_valid]
        df_with_id_cleaned = (
            df_with_id.sort_values('non_empty_count', ascending=False)
                      .drop_duplicates(subset='agent_id', keep='first')
        )
        df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)
        df_cleaned.drop(columns=['non_empty_count'], inplace=True, errors='ignore')
    else:
        df_cleaned = df.copy()

    df_cleaned.columns = df_cleaned.columns.str.lower()

    # hire_date → int YYYYMMDD
    df_cleaned["hire_date"] = pd.to_datetime(df_cleaned["hire_date"], errors='coerce')
    df_cleaned["hire_date"] = df_cleaned["hire_date"].dt.strftime('%Y%m%d').where(df_cleaned["hire_date"].notnull(), None)
    df_cleaned["hire_date"] = df_cleaned["hire_date"].astype('Int64')

    # date_active → datetime / None
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

    # misc fields
    df_cleaned["zipcode"] = df_cleaned["zipcode"].astype(str).where(df_cleaned["zipcode"].astype(str).str.len() == 5, np.nan)
    df_cleaned["agent_name"] = df_cleaned["agent_name"].astype(str).str.lstrip()
    df_cleaned["is_experienced"] = df_cleaned["is_experienced"].apply(
        lambda x: 'no' if str(x).strip().lower() == 'ไม่เคยขาย' else 'yes'
    )
    df_cleaned["job"] = df_cleaned["job"].where(pd.notna(df_cleaned["job"]), None).replace(['NaN','nan','NULL','null'], None)
    df_cleaned["province"] = df_cleaned["province"].replace("Others", "อื่นๆ")

    def clean_email(email):
        if pd.isna(email) or email == '': return None
        email_str = str(email).strip()
        if re.findall(r'[ก-๙]', email_str): return None
        return email_str.lower() if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email_str) else None
    df_cleaned["agent_email"] = df_cleaned["agent_email"].apply(clean_email)

    def clean_agent_name(name):
        if pd.isna(name) or name == '': return None
        s = str(name).strip()
        if s.startswith(('ิ','ี','ึ','ื','ุ','ู','่','้','๊','๋')):
            s = re.sub(r'^[ิีึืุู่้๊๋]+', '', s)
        s = re.sub(r'[^\u0E00-\u0E7F\u0020\u0041-\u005A\u0061-\u007A]', '', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return None if len(s) < 2 else s
    df_cleaned["agent_name"] = df_cleaned["agent_name"].apply(clean_agent_name)

    def clean_store_name(store_name):
        if pd.isna(store_name) or store_name == '': return None
        s = str(store_name).strip()
        return s if any(k in s.lower() for k in ['ร้าน','shop','store','บริษัท','company']) else None
    df_cleaned["store_name"] = df_cleaned["store_name"].apply(clean_store_name)

    def check_thai_text(text):
        if pd.isna(text) or text == '': return None
        cleaned_text = re.sub(r'^[\s\]\*]+', '', str(text).strip())
        for pattern in [r'^\d+$', r'^[A-Za-z\s]+$']:
            if re.match(pattern, cleaned_text):
                return None
        return cleaned_text if re.findall(r'[ก-๙]', cleaned_text) else None
    for col in ['subdistrict','district','province','current_province','current_area']:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(check_thai_text)

    def clean_zipcode(zipcode):
        if pd.isna(zipcode) or zipcode == '': return None
        z = str(zipcode).strip()
        return z if re.match(r'^\d{5}$', z) else None
    df_cleaned["zipcode"] = df_cleaned["zipcode"].apply(clean_zipcode)

    def clean_address(address):
        if pd.isna(address) or address == '': return None
        s = str(address).strip()
        s = re.sub(r'^[\u0E30-\u0E3A\u0E47-\u0E4E]+', '', s)
        s = re.sub(r'[-:.,]', '', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return s
    df_cleaned["agent_address"] = df_cleaned["agent_address"].apply(clean_address).replace(['NaN','None','NULL'], None)

    # อย่าทำความสะอาดคอลัมน์วันที่ในลูปด้านล่าง
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


# =======================
# LOAD (unchanged behavior)
# =======================
@op
def load_to_wh(df: pd.DataFrame):
    table_name = 'dim_agent'
    pk_column = 'agent_id'

    df = df.where(pd.notnull(df), None)

    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

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

    def sanitize_batch_remove_date_active(df_batch: pd.DataFrame) -> list[dict]:
        df_batch = df_batch.where(pd.notnull(df_batch), None)
        recs = []
        for rec in df_batch.to_dict(orient="records"):
            if 'date_active' in rec:
                del rec['date_active']
            recs.append(rec)
        return recs

    # insert / upsert
    if not df_to_insert.empty:
        df_to_insert = df_to_insert[~df_to_insert[pk_column].duplicated(keep='first')].copy()
        with target_engine.begin() as conn:
            for batch_df in chunk_dataframe(df_to_insert):
                records = sanitize_batch_remove_date_active(batch_df.copy())
                record_keys = set(records[0].keys()) if records else set()
                stmt = pg_insert(metadata_table).values(records)
                valid_column_names = [c.name for c in metadata_table.columns]
                update_columns = {
                    c: stmt.excluded[c]
                    for c in valid_column_names
                    if c != pk_column and c in record_keys and c != 'date_active'
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    if 'df_diff_renamed' in locals() and not df_diff_renamed.empty:
        with target_engine.begin() as conn:
            for batch_df in chunk_dataframe(df_diff_renamed):
                records = sanitize_batch_remove_date_active(batch_df.copy())
                record_keys = set(records[0].keys()) if records else set()
                stmt = pg_insert(metadata_table).values(records)
                valid_column_names = [c.name for c in metadata_table.columns]
                update_columns = {
                    c: stmt.excluded[c]
                    for c in valid_column_names
                    if c not in [pk_column, 'id_contact', 'create_at', 'update_at', 'date_active']
                    and c in record_keys
                }
                update_columns['update_at'] = datetime.now()
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed (date_active skipped).")


# =======================
# BACKFILL date_active
# =======================
@op
def backfill_date_active(df: pd.DataFrame):
    table_name = 'dim_agent'
    pk = 'agent_id'

    if 'date_active' not in df.columns:
        print("⚠️ Input df has no date_active column — skip backfill")
        return

    s = pd.to_datetime(df['date_active'], errors='coerce')
    try:
        s = s.dt.tz_localize(None)
    except Exception:
        pass

    df_dates = pd.DataFrame({pk: df[pk].astype(str).str.strip(), 'date_active': s})
    df_dates = df_dates[df_dates[pk].astype(bool)].copy()

    df_dates['__rank'] = df_dates['date_active'].notna().astype(int)
    df_dates = df_dates.sort_values([pk, '__rank', 'date_active'], ascending=[True, False, False])
    df_dates = df_dates.drop(columns='__rank').drop_duplicates(subset=[pk], keep='first')

    def _coerce_py_datetime(v):
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        if isinstance(v, np.datetime64):
            try:
                return pd.Timestamp(v).to_pydatetime()
            except Exception:
                return None
        if isinstance(v, str):
            if v.strip().lower() == 'nat': return None
            try:
                return pd.Timestamp(v).to_pydatetime()
            except Exception:
                return None
        if isinstance(v, datetime):
            return v
        return None

    df_dates['date_active'] = df_dates['date_active'].apply(_coerce_py_datetime)
    df_dates = df_dates.replace({pd.NaT: None, 'NaT': None})

    print(f"🔎 date_active rows total={len(df_dates)}, null/None={df_dates['date_active'].isna().sum()}, not-null={len(df_dates) - df_dates['date_active'].isna().sum()}")

    metadata_table = Table('dim_agent', MetaData(), autoload_with=target_engine)

    def chunk(dfx, n=500):
        for i in range(0, len(dfx), n):
            yield dfx.iloc[i:i+n]

    total = 0
    with target_engine.begin() as conn:
        for b in chunk(df_dates[[pk, 'date_active']]):
            records = []
            for rec in b.to_dict(orient='records'):
                rec['date_active'] = _coerce_py_datetime(rec.get('date_active'))
                records.append(rec)

            stmt = pg_insert(metadata_table).values(records)
            cols = [c.name for c in metadata_table.columns]
            set_map = {'date_active': stmt.excluded['date_active']}
            if 'update_at' in cols:
                set_map['update_at'] = datetime.now()
            stmt = stmt.on_conflict_do_update(index_elements=[pk], set_=set_map)
            conn.execute(stmt)
            total += len(records)

    print(f"✅ Backfilled date_active for {total} agents")


# =======================
# JOB
# =======================
@job
def dim_agent_etl():
    df_clean = clean_agent_data(extract_agent_data())
    load_to_wh(df_clean)
    backfill_date_active(df_clean)


# =======================
# MAIN
# =======================
if __name__ == "__main__":
    df_raw = extract_agent_data()
    print("✅ Extracted logs:", df_raw.shape)

    df_clean = clean_agent_data(df_raw)
    print("✅ Cleaned columns:", df_clean.columns)

    # บันทึกผลลัพธ์ (ใช้ timestamp กันชนกับไฟล์ที่ถูกเปิดค้าง)
    out_main = f"dim_agent_{timestamp_str()}.xlsx"
    try:
        df_clean.to_excel(out_main, index=False, engine='openpyxl')
        print(f"💾 Saved to {out_main}")
    except Exception as e:
        print(f"⚠️ Save main failed: {e}")

    # ต่อด้วยโหลดขึ้นคลัง และ backfill
    # load_to_wh(df_clean)
    # backfill_date_active(df_clean)
    print("🎉 completed! ETL finished.")
