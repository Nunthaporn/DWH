from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime

# ✅ โหลด .env
load_dotenv()

# ✅ DB source (MariaDB) : fininsurance
source_user = os.getenv('DB_USER')
source_password = os.getenv('DB_PASSWORD')
source_host = os.getenv('DB_HOST')
source_port = os.getenv('DB_PORT')
source_db = 'fininsurance'

source_engine = create_engine(
    f"mysql+pymysql://{source_user}:{source_password}@{source_host}:{source_port}/{source_db}",
    pool_pre_ping=True
)

# ✅ DB target (PostgreSQL) : fininsurance
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
        "options": "-c statement_timeout=900000"  # 15 นาที
    },
    pool_pre_ping=True
)

# -----------------------------
#           EXTRACT
# -----------------------------
@op
def extract_agent_data():
    # ใช้ CASE เพื่อแก้วันที่ 0000...
    query_main = text("""
        SELECT cuscode, name, rank,
               CASE 
                   WHEN user_registered = '0000-00-00 00:00:00.000' THEN '2000-01-01 00:00:00'
                   ELSE user_registered 
               END AS user_registered,
               status, fin_new_group, fin_new_mem,
               type_agent, typebuy, user_email, name_store, address, city, district,
               province, province_cur, area_cur, postcode, tel, date_active
        FROM wp_users
        WHERE user_login NOT IN ('FINTEST-01', 'FIN-TestApp', 'Admin-VIF', 'adminmag_fin', 'FNG00-00001')
            AND name NOT LIKE '%%ทดสอบ%%'
            AND name NOT LIKE '%%tes%%'
            -- AND cuscode NOT LIKE 'ADMIN-SALE001%%'
            -- AND cuscode NOT LIKE 'center_sale%%'
            -- AND cuscode NOT LIKE 'Client-sale%%'
            -- AND cuscode NOT LIKE 'Sale%%'
            -- AND cuscode NOT LIKE 'south%%'
            -- AND cuscode NOT LIKE 'mng_sale%%'
            -- AND cuscode NOT LIKE 'bkk%%'
            -- AND cuscode NOT LIKE 'east%%'
            -- AND cuscode NOT LIKE 'north%%'
            -- AND cuscode NOT LIKE 'central%%'
            -- AND cuscode NOT LIKE 'upc%%'
            -- AND cuscode NOT LIKE 'sqc_%%'
            -- AND cuscode NOT LIKE 'pm_%%'
            -- AND cuscode NOT LIKE 'Sale-Tor%%'
            -- AND cuscode NOT LIKE 'online%%'
            -- AND cuscode NOT LIKE 'Sale-Direct%%'
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

    # อาชีพจาก policy_register
    query_career = text("SELECT cuscode, career FROM policy_register")
    df_career = pd.read_sql(query_career, source_engine)

    # mapping quotation_num จาก fin_system_select_plan
    query_plan = text("SELECT quo_num, id_cus FROM fin_system_select_plan WHERE datestart BETWEEN '2025-01-01' AND '2025-08-31'")
    df_plan = pd.read_sql(query_plan, source_engine)
    df_plan = df_plan.rename(columns={'quo_num': 'quotation_num', 'id_cus': 'agent_id'})

    print(f"📦 df_main: {df_main.shape} | df_career: {df_career.shape} | df_plan: {df_plan.shape}")
    return df_main, df_career, df_plan

# -----------------------------
#           CLEAN
# -----------------------------
@op
def clean_agent_data(extracted_tuple):
    df_main, df_career, df_plan = extracted_tuple

    # ---- รวมข้อมูลอาชีพ
    df = pd.merge(df_main, df_career, on='cuscode', how='left')
    df['career'] = df['career'].astype(str).str.strip()

    # ---- Normalize defect & cuscode
    df['cuscode'] = df['cuscode'].astype(str).str.strip()
    df['status'] = df['status'].astype(str).str.strip().str.lower()

    is_defect_initial = (
        df['cuscode'].str.contains(r'-defect$', case=False, na=False) |
        df['status'].eq('defect')
    )
    base_id = df['cuscode'].str.replace(r'-defect$', '', regex=True)
    df['cuscode'] = np.where(is_defect_initial, base_id + '-defect', base_id)

    # ---- รวม region & main_region
    def combine_columns(a, b):
        a_str = str(a).strip() if pd.notna(a) else ''
        b_str = str(b).strip() if pd.notna(b) else ''
        if a_str == '' and b_str == '': return ''
        if a_str == '': return b_str
        if b_str == '': return a_str
        return a_str if a_str == b_str else f"{a_str} + {b_str}"

    df['agent_region'] = df.apply(lambda r: combine_columns(r['fin_new_group'], r['fin_new_mem']), axis=1)
    df['agent_main_region'] = (
        df['agent_region'].fillna('').astype(str).str.replace(r'\d+', '', regex=True).str.strip()
    )
    df = df[df['agent_region'] != 'TEST'].copy()
    df = df.drop(columns=['fin_new_group', 'fin_new_mem'], errors='ignore')

    # ---- Rename columns
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

    # ---- defect_status หลัง rename
    is_defect_after = (
        df['agent_id'].str.contains(r'-defect$', case=False, na=False) |
        df['status'].astype(str).str.strip().str.lower().eq('defect')
    )
    df['defect_status'] = np.where(is_defect_after, 'defect', None)
    df = df.drop(columns=['status'], errors='ignore')

    # ---- Cleaning fields
    valid_rank = [str(i) for i in range(1, 11)]
    df.loc[~df['agent_rank'].isin(valid_rank), 'agent_rank'] = np.nan

    # is_experienced: 'ไม่เคยขาย' -> 'no', อื่นๆ -> 'yes'
    df['is_experienced'] = df['is_experienced'].apply(
        lambda x: 'no' if str(x).strip().lower() == 'ไม่เคยขาย' else 'yes'
    )

    # เบอร์โทร เก็บเฉพาะตัวเลข
    df['mobile_number'] = df['mobile_number'].astype(str).str.replace(r'[^0-9]', '', regex=True)

    # ลบคำบอกตำแหน่งถนน/ซอย ฯลฯ ใน address
    df['agent_address'] = df['agent_address'].apply(
        lambda addr: re.sub(r'(เลขที่|หมู่ที่|หมู่บ้าน|ซอย|ถนน)[\s\-]*', '', str(addr)).strip() if pd.notna(addr) else None
    )

    # แทนค่าว่างด้วย NA
    df = df.replace(r'^\s*$', np.nan, regex=True)

    # เลือก record ที่ข้อมูลแน่นสุดต่อ agent_id
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

    # ---- hire_date -> int YYYYMMDD
    df_cleaned['hire_date'] = pd.to_datetime(df_cleaned['hire_date'], errors='coerce')
    df_cleaned['hire_date'] = df_cleaned['hire_date'].dt.strftime('%Y%m%d').where(df_cleaned['hire_date'].notnull(), None)
    df_cleaned['hire_date'] = df_cleaned['hire_date'].astype('Int64')

    # ---- date_active ให้เป็น datetime (หรือ None)
    if 'date_active' in df_cleaned.columns:
        dt = pd.to_datetime(df_cleaned['date_active'], errors='coerce')
        try:
            dt = dt.dt.tz_localize(None)
        except Exception:
            pass
        df_cleaned['date_active'] = [
            (v.to_pydatetime() if isinstance(v, pd.Timestamp) and pd.notna(v) else
             (v if isinstance(v, datetime) else None))
            for v in dt
        ]

    # ---- ฟังก์ชันทำความสะอาดเฉพาะ field เพิ่มเติม
    df_cleaned['zipcode'] = df_cleaned['zipcode'].apply(
        lambda z: (str(z).strip() if (pd.notna(z) and re.match(r'^\d{5}$', str(z).strip())) else None)
    )
    df_cleaned['agent_name'] = df_cleaned['agent_name'].astype(str).str.lstrip()

    def clean_email(email):
        if pd.isna(email) or email == '': return None
        email_str = str(email).strip()
        if re.findall(r'[ก-๙]', email_str):  # ไม่รับอักษรไทยในอีเมล
            return None
        return email_str.lower() if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email_str) else None
    df_cleaned['agent_email'] = df_cleaned['agent_email'].apply(clean_email)

    def clean_agent_name(name):
        if pd.isna(name) or name == '': return None
        s = str(name).strip()
        if s.startswith(('ิ','ี','ึ','ื','ุ','ู','่','้','๊','๋')):
            s = re.sub(r'^[ิีึืุู่้๊๋]+', '', s)
        s = re.sub(r'[^\u0E00-\u0E7F\u0020\u0041-\u005A\u0061-\u007A]', '', s)  # ไทย/อังกฤษ/ช่องว่าง
        s = re.sub(r'\s+', ' ', s).strip()
        return None if len(s) < 2 else s
    df_cleaned['agent_name'] = df_cleaned['agent_name'].apply(clean_agent_name)

    def clean_store_name(store_name):
        if pd.isna(store_name) or store_name == '': return None
        s = str(store_name).strip()
        return s if any(k in s.lower() for k in ['ร้าน','shop','store','บริษัท','company']) else None
    df_cleaned['store_name'] = df_cleaned['store_name'].apply(clean_store_name)

    def check_thai_text(text):
        if pd.isna(text) or text == '': return None
        cleaned = re.sub(r'^[\s\]\*]+', '', str(text).strip())
        for pattern in [r'^\d+$', r'^[A-Za-z\s]+$']:
            if re.match(pattern, cleaned):
                return None
        return cleaned if re.findall(r'[ก-๙]', cleaned) else None

    for col in ['subdistrict', 'district', 'province', 'current_province', 'current_area']:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(check_thai_text)

    # อย่าทำความสะอาดคอลัมน์วันที่ในลูปด้านล่าง
    date_cols = {'date_active'}
    for col in df_cleaned.columns:
        if col in {'agent_id'} | date_cols:
            continue
        if df_cleaned[col].dtype == 'object':
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: (re.sub(r'^\s+', '', str(x).strip()) or None) if pd.notna(x) and x != '' else None
            )

    # ---- Merge กับ fin_system_select_plan เพื่อได้ quotation_num
    df_result = pd.merge(df_cleaned, df_plan, on='agent_id', how='right')
    # ลบแถวที่ไม่มี quotation_num
    # df_result = df_result.dropna(subset=['quotation_num']).copy()

    patterns_to_remove = ['FIN-TestApp', 'FIN-TestApp2', 'FIN-TestApp3', 'FIN-TestApp-2025']
    df_result = df_result[~df_result['agent_id'].astype(str).str.contains('|'.join(patterns_to_remove), case=False, na=False)]

    print("📊 Cleaning completed:", df_result.shape)
    return df_result

# -----------------------------
#           LOAD
# -----------------------------
@op
def load_to_wh(df: pd.DataFrame):
    table_name = 'dim_agent_temp'
    pk_column = 'quotation_num'

    # --- sanitize รอบสุดท้าย ---
    df = df.where(pd.notnull(df), None)
    df = df[~df[pk_column].duplicated(keep='first')].copy()

    metadata_table = Table(table_name, MetaData(), autoload_with=target_engine)

    def chunk_dataframe(dfx, chunk_size=100000):  # เพิ่ม chunk ให้ใหญ่ขึ้น
        for i in range(0, len(dfx), chunk_size):
            yield dfx.iloc[i:i + chunk_size], i // chunk_size + 1

    # helper: sanitize per-batch & ลบ date_active
    def sanitize_batch_remove_date_active(df_batch: pd.DataFrame) -> list[dict]:
        df_batch = df_batch.where(pd.notnull(df_batch), None)
        recs = []
        for rec in df_batch.to_dict(orient="records"):
            if 'date_active' in rec:
                del rec['date_active']
            recs.append(rec)
        return recs

    total_rows = len(df)
    total_batches = (total_rows // 100000) + (1 if total_rows % 100000 else 0)
    inserted_rows = 0

    with target_engine.begin() as conn:
        for batch_df, batch_num in chunk_dataframe(df):
            records = sanitize_batch_remove_date_active(batch_df.copy())
            if not records:
                continue
            stmt = metadata_table.insert().values(records)
            conn.execute(stmt)
            inserted_rows += len(records)
            print(f"📦 Batch {batch_num}/{total_batches} inserted {len(records)} rows "
                  f"(total {inserted_rows}/{total_rows})")

    print(f"✅ Insert completed: {inserted_rows} rows into {table_name}")

@op
def backfill_date_active(df: pd.DataFrame):
    """เติมค่า date_active ย้อนหลังแบบปลอดภัย หลังจาก upsert หลักเสร็จแล้ว"""
    table_name = 'dim_agent_temp'
    pk = 'quotation_num'

    if 'date_active' not in df.columns:
        print("⚠️ Input df has no date_active column — skip backfill")
        return

    # 1) parse -> pandas datetime (coerce) และลบ timezone
    s = pd.to_datetime(df['date_active'], errors='coerce')
    try:
        s = s.dt.tz_localize(None)
    except Exception:
        pass

    df_dates = pd.DataFrame({
        pk: df[pk].astype(str).str.strip(),
        'date_active': s
    })

    # เก็บเฉพาะ agent_id ที่ไม่ว่าง
    df_dates = df_dates[df_dates[pk].astype(bool)].copy()

    # 2) dedupe: ให้ not-null มาก่อน และเอาเวลาที่ล่าสุด
    df_dates['__rank'] = df_dates['date_active'].notna().astype(int)
    df_dates = df_dates.sort_values([pk, '__rank', 'date_active'], ascending=[True, False, False])
    df_dates = df_dates.drop(columns='__rank').drop_duplicates(subset=[pk], keep='first')

    # 3) HARDEN: แปลงทุกค่าให้เป็น python datetime หรือ None (กำจัด NaT/สตริง 'NaT')
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
            if v.strip().lower() == 'nat':
                return None
            try:
                return pd.Timestamp(v).to_pydatetime()
            except Exception:
                return None
        if isinstance(v, datetime):
            return v
        return None

    df_dates['date_active'] = df_dates['date_active'].apply(_coerce_py_datetime)
    df_dates = df_dates.replace({pd.NaT: None, 'NaT': None})

    # (ถ้าไม่อยาก overwrite ด้วย NULL ทับของเดิม ให้เปิดบรรทัดนี้)
    # df_dates = df_dates[df_dates['date_active'].notna()].copy()

    # log ตรวจสอบ
    n_total = len(df_dates)
    n_null = df_dates['date_active'].isna().sum()
    print(f"🔎 date_active rows total={n_total}, null/None={n_null}, not-null={n_total - n_null}")

    metadata_table = Table(table_name, MetaData(), autoload_with=target_engine)

    def chunk(dfx, n=100000):
        for i in range(0, len(dfx), n):
            yield dfx.iloc[i:i+n]

    total = 0
    with target_engine.begin() as conn:
        for b in chunk(df_dates[[pk, 'date_active']]):
            # ✅ กันพลาดรอบสุดท้าย: บังคับ coerce อีกรอบตอนสร้าง records
            records = []
            for rec in b.to_dict(orient='records'):
                v = rec.get('date_active')
                rec['date_active'] = _coerce_py_datetime(v)
                records.append(rec)

            # ถ้า batch มีแต่ None หมดและไม่อยากอัปเดต ให้ข้าม (เลือกใช้ได้)
            # if all(r['date_active'] is None for r in records):
            #     continue

            stmt = pg_insert(metadata_table).values(records)
            cols = [c.name for c in metadata_table.columns]
            set_map = {'date_active': stmt.excluded['date_active']}
            if 'update_at' in cols:
                set_map['update_at'] = datetime.now()

            stmt = stmt.on_conflict_do_update(index_elements=[pk], set_=set_map)
            conn.execute(stmt)
            total += len(records)

    print(f"✅ Backfilled date_active for {total} agents")

@op
def clean_null_values_op(df: pd.DataFrame) -> pd.DataFrame:
    # รวม 'NaT' ด้วย ป้องกัน string หลุดรอด
    return df.replace(['None', 'none', 'nan', 'NaN', 'NaT', ''], np.nan)


@job
def dim_agent_etl():
    df_clean = clean_agent_data(clean_null_values_op(extract_agent_data()))
    load_to_wh(df_clean)            # upsert คอลัมน์หลัก (ไม่เขียน date_active)
    backfill_date_active(df_clean)  # เติม date_active ทีหลังแบบปลอดภัย


if __name__ == "__main__":
    df_raw_tuple = extract_agent_data()
    # log รูปทรงแบบถูกต้อง
    df_main, df_career, df_plan = df_raw_tuple
    print(f"✅ Extracted shapes: df_main={df_main.shape}, df_career={df_career.shape}, df_plan={df_plan.shape}")

    df_clean = clean_agent_data(df_raw_tuple)
    print(f"✅ Cleaned columns count: {len(df_clean.columns)}")
    # ถ้าจะดูตัวอย่างคอลัมน์:
    # print(sorted(df_clean.columns.tolist()))

    df_clean = clean_null_values_op(df_clean)
    print(f"✅ clean_null_values_op")

    # df_clean.to_csv("agent_data_clean.csv", index=False, encoding="utf-8-sig")
    # print(f"✅ Saved to agent_data_clean.csv")

    load_to_wh(df_clean)
    print(f"✅ success load_to_wh")

    backfill_date_active(df_clean)
    print(f"✅ success backfill_date_active")

    print("🎉 completed! Data")
