# %%
from dagster import op, job
import pandas as pd
import numpy as np
import os, re, time, random
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# ✅ Load .env
load_dotenv()

# ✅ DB connections
# Sources (MariaDB)
src_fin = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600,
    connect_args={"connect_timeout": 30}
)
src_task = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600,
    connect_args={"connect_timeout": 30}
)

# Target (PostgreSQL)
tgt = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600,
    connect_args={"connect_timeout": 30, "application_name": "payment_plan_id_update"}
)

# ---------- helpers ----------
def _strip_lower(s):
    return "" if s is None or (isinstance(s, float) and pd.isna(s)) else str(s).strip().lower()

def _safe_to_int(series: pd.Series) -> pd.Series:
    out = pd.to_numeric(series, errors='coerce').fillna(0).astype(int)
    out = out.replace({0: 1})  # ตามกฎเดิม: 0 -> 1
    return out

def _nan_string_to_nan(df: pd.DataFrame) -> pd.DataFrame:
    return df.map(lambda x: np.nan if isinstance(x, str) and x.strip().lower() == "nan" else x)

# ---------- EXTRACT ----------
@op
def extract_select_plan_id() -> pd.DataFrame:
    with src_fin.connect() as conn:
        df = pd.read_sql("SELECT quo_num, type_insure FROM fin_system_select_plan", conn)
    print(f"📦 select_plan: {df.shape}")
    return df

@op
def extract_pay() -> pd.DataFrame:
    with src_fin.connect() as conn:
        df = pd.read_sql("""
            SELECT quo_num, chanel_main, clickbank, chanel, numpay, condition_install
            FROM fin_system_pay
        """, conn)
    print(f"📦 pay: {df.shape}")
    return df

@op
def extract_order_status() -> pd.DataFrame:
    with src_task.connect() as conn:
        df = pd.read_sql("SELECT quo_num, status_paybill FROM fin_order", conn)
    print(f"📦 order_status: {df.shape}")
    return df

@op
def extract_dim_payment_plan() -> pd.DataFrame:
    with tgt.connect() as conn:
        df = pd.read_sql(text("""
            SELECT payment_plan_id, payment_channel, payment_reciever, payment_type, installment_number
            FROM dim_payment_plan
        """), conn)
    print(f"📦 dim_payment_plan: {df.shape}")
    return df

@op
def extract_fsq_missing_payment_plan() -> pd.DataFrame:
    with tgt.connect() as conn:
        df = pd.read_sql(text("""
            SELECT quotation_num
            FROM fact_sales_quotation
            WHERE payment_plan_id IS NULL
        """), conn)
    print(f"📦 fsq missing payment_plan_id: {df.shape}")
    return df

# ---------- TRANSFORM ----------
@op
def build_payment_facts(df_plan: pd.DataFrame, df_pay: pd.DataFrame, df_orderstatus: pd.DataFrame) -> pd.DataFrame:
    # 1) pre-clean
    df_pay = df_pay.copy()
    df_pay['chanel'] = df_pay['chanel'].replace({'ผ่อนบัตร': 'เข้าฟิน'})  # normalize
    merged = pd.merge(df_pay, df_orderstatus, on='quo_num', how='left')
    merged = _nan_string_to_nan(merged)

    # 2) strings cleanup
    for col in ['chanel', 'chanel_main', 'clickbank']:
        merged[col] = merged[col].fillna('').astype(str).str.strip()

    # 3) rewrite chanel ตาม rule (np.select)
    ch = merged['chanel'].str.lower()
    chm = merged['chanel_main'].str.lower()
    cb = merged['clickbank'].str.lower()

    conditions = [
        ch == 'เข้าฟิน',
        ch == 'เข้าประกัน',
        (chm.isin(['ผ่อนบัตรเครดิต', 'ผ่อนบัตร']) & cb.isin(['creditcard', '']) & ch.eq('ผ่อนบัตร')),
        (chm.isin(['ผ่อนบัตร']) & cb.isin(['creditcard', '']) & ch.eq('ผ่อนบัตรเครดิต')),
        (chm.eq('ตัดบัตรเครดิต') & cb.isin(['']) & ch.eq('ผ่อนบัตร')),
        (chm.eq('ผ่อนโอน') & cb.isin(['qrcode']) & ch.eq('ผ่อนโอน')),
        (chm.eq('ผ่อนโอน') & cb.str.startswith('ธนาคาร') & ch.eq('ผ่อนบัตร')),
        (chm.eq('ตัดบัตรเครดิต') & cb.isin(['creditcard', '']) & ch.eq('ออนไลน์')),
        (chm.eq('ตัดบัตรเครดิต') & cb.str.startswith('ธนาคาร') & ch.eq('ออนไลน์')),
        (chm.eq('ผ่อนบัตรเครดิต') & cb.isin(['qrcode','creditcard','']) & ch.eq('ออนไลน์')),
        (chm.eq('ผ่อนบัตรเครดิต') & cb.str.startswith('ธนาคาร') & ch.eq('ออนไลน์')),
        (chm.eq('โอนเงิน') & cb.str.startswith('ธนาคาร') & ch.eq('ออนไลน์')),
        (chm.eq('ตัดบัตรเครดิต') & cb.eq('') & ch.eq('ตัดบัตรเครดิต')),
        (chm.eq('ผ่อนชำระ') & (cb.isin(['qrcode', '']) | cb.str.startswith('ธนาคาร')) & ch.eq('ผ่อนโอน')),
        (chm.eq('ผ่อนโอน') & cb.str.startswith('ธนาคาร') & ch.eq('ผ่อนโอน')),
        (chm.eq('ผ่อนโอน') & cb.str.startswith('ธนาคาร') & ch.eq('ออนไลน์')),
        (chm.eq('ตัดบัตรเครดิต') & cb.eq('') & ch.eq('ผ่อนโอน')),
        (chm.eq('ผ่อนโอน') & cb.eq('') & ch.eq('ผ่อนโอน')),
        (chm.eq('ผ่อนบัตรเครดิต') & cb.eq('ธนาคารกรุงไทย') & ch.eq('ผ่อนบัตร')),
        (chm.eq('ตัดบัตรเครดิต') & cb.eq('creditcard') & ch.eq('ผ่อนบัตร')),
        (chm.eq('ตัดบัตรเครดิต') & cb.eq('creditcard') & ch.eq('ตัดบัตรเครดิต')),
    ]
    choices = ['เข้าฟิน', 'เข้าประกัน'] + ['เข้าฟิน'] * (len(conditions) - 2)
    merged['chanel'] = np.select(conditions, choices, default=merged['chanel'])

    # 4) derive payment_channel
    def determine_payment_channel(row):
        ch_main = _strip_lower(row['chanel_main'])
        cb_raw = row['clickbank']; cb = _strip_lower(cb_raw)
        is_cb_empty = (cb_raw is None) or (cb == '')
        if ch_main in ['ตัดบัตรเครดิต', 'ผ่อนบัตร', 'ผ่อนบัตรเครดิต', 'ผ่อนชำระ']:
            if 'qrcode' in cb:  return 'QR Code'
            if 'creditcard' in cb: return '2C2P'
            return 'ตัดบัตรกับฟิน'
        if ch_main in ['โอนเงิน', 'ผ่อนโอน']:
            if 'qrcode' in cb: return 'QR Code'
            return 'โอนเงิน'
        if ch_main and is_cb_empty:
            return row['chanel_main']
        if not ch_main and not is_cb_empty:
            if 'qrcode' in cb: return 'QR Code'
            if 'creditcard' in cb: return '2C2P'
            return row['clickbank']
        if not is_cb_empty:
            if 'qrcode' in cb: return 'QR Code'
            if 'creditcard' in cb: return '2C2P'
            return row['clickbank']
        return ''

    merged['payment_channel'] = merged.apply(determine_payment_channel, axis=1)

    # 5) tidy / rename to canonical columns for join
    merged = merged.drop(columns=['chanel_main', 'clickbank', 'condition_install'], errors='ignore')
    merged = merged.rename(columns={
        'quo_num': 'quotation_num',
        'type_insure': 'type_insurance',
        'chanel': 'payment_reciever',
        'status_paybill': 'payment_type',
        'numpay': 'installment_number',
    })
    merged['payment_reciever'] = merged['payment_reciever'].replace({'เข้าประกัน1': 'เข้าประกัน', 'เข้าฟินลิป': 'เข้าฟิลลิป'})
    merged['installment_number'] = _safe_to_int(merged['installment_number'])

    # join type_insurance (optional: for diagnostic)
    merged = pd.merge(
        merged,
        df_plan.rename(columns={'quo_num': 'quotation_num', 'type_insure': 'type_insurance'}),
        on='quotation_num', how='left', suffixes=('', '_sp')
    )
    # prefer existing if not null else plan’s type
    merged['type_insurance'] = np.where(
        merged['type_insurance'].notna(), merged['type_insurance'], merged['type_insurance_sp']
    )
    merged.drop(columns=['type_insurance_sp'], inplace=True)

    keep = ['quotation_num', 'payment_channel', 'payment_reciever', 'payment_type', 'installment_number', 'type_insurance']
    merged = merged[keep].drop_duplicates()
    print(f"🧹 payment facts (for mapping): {merged.shape}")
    return merged

@op
def attach_payment_plan_id(df_payment_facts: pd.DataFrame, dim_payment_plan: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(
        df_payment_facts,
        dim_payment_plan,
        on=["payment_channel", "payment_reciever", "payment_type", "installment_number"],
        how="inner"
    )
    df = df[['quotation_num', 'payment_plan_id']].drop_duplicates()
    print(f"🔗 mapped to dim_payment_plan: {df.shape}")
    return df

@op
def filter_to_missing_fsq(df_map: pd.DataFrame, fsq_missing: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(df_map, fsq_missing, on='quotation_num', how='right')
    # diagnostics
    if 'payment_plan_id' in df.columns:
        null_count = df['payment_plan_id'].isna().sum()
        print(f"ℹ️ NULL payment_plan_id after FSQ filter: {null_count} / {len(df)}")
        if null_count > 0:
            try:
                df[df['payment_plan_id'].isna()].head(1000).to_excel("payment_plan_id_null.xlsx", index=False)
                print("💾 saved examples to payment_plan_id_null.xlsx")
            except Exception:
                pass
    else:
        print("❌ payment_plan_id column not found.")
    df = df[['quotation_num', 'payment_plan_id']].copy()
    for c in ['quotation_num', 'payment_plan_id']:
        df[c] = df[c].astype('string').str.strip()
    df = df.dropna(subset=['quotation_num']).drop_duplicates(subset=['quotation_num'])
    print(f"✅ update candidates: {df.shape}")
    return df

# ---------- LOAD (UPDATE) ----------
@op
def update_payment_plan_id(df_updates: pd.DataFrame) -> None:
    if df_updates.empty:
        print("ℹ️ No rows to update.")
        return
    with tgt.begin() as conn:
        conn.exec_driver_sql("SET lock_timeout = '3s'")
        conn.exec_driver_sql("SET deadlock_timeout = '200ms'")
        conn.exec_driver_sql("SET statement_timeout = '60s'")

        conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_payment_plan_updates(
                quotation_num text PRIMARY KEY,
                payment_plan_id text
            ) ON COMMIT DROP
        """)

        df_updates.to_sql(
            "tmp_payment_plan_updates",
            con=conn, if_exists="append", index=False,
            method="multi", chunksize=10_000
        )

        update_sql = text("""
            UPDATE fact_sales_quotation f
            SET payment_plan_id = t.payment_plan_id,
                update_at       = NOW()
            FROM tmp_payment_plan_updates t
            WHERE f.quotation_num = t.quotation_num
              AND (f.payment_plan_id IS NULL OR f.payment_plan_id IS DISTINCT FROM t.payment_plan_id)
        """)

        max_retries = 5
        for attempt in range(max_retries):
            try:
                result = conn.execute(update_sql)
                print(f"🚀 Updated rows: {result.rowcount}")
                break
            except OperationalError as e:
                msg = str(getattr(e, "orig", e)).lower()
                if "deadlock detected" in msg and attempt < max_retries - 1:
                    sleep_s = (2 ** attempt) + random.random()
                    print(f"⚠️ Deadlock detected. Retrying in {sleep_s:.2f}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(sleep_s)
                    continue
                raise
        print("✅ Update payment_plan_id completed.")

# ---------- JOB ----------
@job
def update_fact_sales_quotation_payment_plan_id():
    update_payment_plan_id(
        filter_to_missing_fsq(
            attach_payment_plan_id(
                build_payment_facts(
                    extract_select_plan_id(),
                    extract_pay(),
                    extract_order_status()
                ),
                extract_dim_payment_plan()
            ),
            extract_fsq_missing_payment_plan()
        )
    )

if __name__ == "__main__":
    sp = extract_select_plan_id()
    pay = extract_pay()
    st = extract_order_status()
    facts = build_payment_facts(sp, pay, st)
    dim = extract_dim_payment_plan()
    mapped = attach_payment_plan_id(facts, dim)
    fsq = extract_fsq_missing_payment_plan()
    updates = filter_to_missing_fsq(mapped, fsq)
    update_payment_plan_id(updates)
    print("🎉 completed! payment_plan_id updated.")
