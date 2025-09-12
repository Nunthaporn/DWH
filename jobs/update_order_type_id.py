# %%
from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
import time
import random
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# ✅ Load .env
load_dotenv()

# ✅ DB Connections
# Source (MariaDB)
src_engine_fin = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600,
    connect_args={"connect_timeout": 30}
)
src_engine_task = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600,
    connect_args={"connect_timeout": 30}
)

# Target (PostgreSQL)
tgt_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600,
    connect_args={"connect_timeout": 30, "application_name": "dim_order_type_update"}
)

# ---------- helpers ----------
def _norm(s):
    if pd.isna(s):
        return ""
    return str(s).strip()

def _lower(s):
    return _norm(s).lower()

BASE_MAP = [
    (r"\bapp\b|application|mobile|แอป", "APP"),
    (r"\bweb\b|website|เวบ|เว็บไซต์", "WEB"),
]
SUBTYPE_SET = {"B2B", "B2C", "TELE", "THAIPOST", "THAICARE"}

DEFAULT_SUBTYPE_APP = {
    "ประกันโควิด": "B2B",
    "ประกันชีวิต": "B2B",
    "ประกันเบ็ดเตล็ด": "B2B",
    "ประกันโรคร้ายแรง": "B2B",
    "ประกันสุขภาพกลุ่ม": "B2B",
    "ประกันอัคคีภัยsme": "B2B",
    "ประกันอัคคีภัยทั่วไป": "B2B",
    "ประกันอุบัติเหตุกลุ่ม": "B2B",
}
DEFAULT_SUBTYPE_WEB = {
    "ประกันโควิด": "B2B",
    "ประกันชีวิต": "B2B",
}

def base_from_type_key(text: str) -> str | None:
    low = _lower(text)
    for pat, label in BASE_MAP:
        if re.search(pat, low):
            return label
    return None

def normalize_special_subtype(raw: str) -> str:
    s = _norm(raw).upper()
    s = s.replace("WEB-AFF", "AFF").replace("WEB AFF", "AFF")
    return s

def extract_subtype(raw: str) -> str | None:
    s = normalize_special_subtype(raw)
    if not s:
        return None
    tokens = re.split(r"[ \-_/]+", s.upper())
    for tok in tokens:
        t = tok.strip()
        if t in SUBTYPE_SET:
            return t
    s_up = s.upper()
    for _, base_label in BASE_MAP:
        s_up = re.sub(rf"\b{re.escape(base_label)}\b", "", s_up, flags=re.IGNORECASE)
    s_up = re.sub(r"\s+", " ", s_up).strip()
    return s_up if s_up else None

def derive_base(row) -> str | None:
    return base_from_type_key(row.get("type_key", ""))

def derive_subtype(row) -> str | None:
    ch_val = row.get("chanel_key", "")
    app_val = row.get("app_type", "")
    sub = extract_subtype(ch_val)
    if sub:
        return sub
    sub = extract_subtype(app_val)
    if sub:
        return sub
    blob = " ".join([
        _lower(ch_val), _lower(app_val),
        _lower(row.get("type_key", "")),
        _lower(row.get("type_insure", "")),
        _lower(row.get("worksend", "")),
    ])
    if ("vif" in blob) or ("ตรอ" in blob):
        return "VIF" if "VIF" in SUBTYPE_SET else None
    return None

def default_subtype_by_product(base: str | None, type_insure: str | None) -> str | None:
    if not base:
        return None
    name = _lower(type_insure)
    if base == "APP":
        for k, v in DEFAULT_SUBTYPE_APP.items():
            if _lower(k) == name:
                return v
    if base == "WEB":
        for k, v in DEFAULT_SUBTYPE_WEB.items():
            if _lower(k) == name:
                return v
    return None

def parse_channel(row):
    base = derive_base(row)
    subtype = derive_subtype(row)
    if not subtype:
        subtype = default_subtype_by_product(base, row.get("type_insure", ""))
    if not base and subtype == "VIF":
        base = "WEB"
    if base in {"APP", "WEB"} and subtype:
        return f"{base} {subtype}"
    if base and not subtype:
        return base
    if subtype and not base:
        return subtype
    return ""

# ---------- EXTRACT ----------
@op
def extract_select_plan() -> pd.DataFrame:
    q = """
    SELECT quo_num,type_insure,type_work,type_status,type_key,app_type,chanel_key,token,in_advance,check_tax
    FROM fin_system_select_plan
    """
    with src_engine_fin.connect() as conn:
        df = pd.read_sql(q, conn)
    print(f"📦 df_select_plan: {df.shape}")
    return df

@op
def extract_order_task() -> pd.DataFrame:
    q = "SELECT quo_num, worksend FROM fin_order"
    with src_engine_task.connect() as conn:
        df = pd.read_sql(q, conn)
    print(f"📦 df_order_task: {df.shape}")
    return df

@op
def extract_dim_order_type() -> pd.DataFrame:
    with tgt_engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT order_type_id, type_insurance, order_type, check_type, work_type, key_channel FROM dim_order_type"),
            conn
        )
    print(f"📦 dim_order_type: {df.shape}")
    return df

@op
def extract_fsq_missing_order_type() -> pd.DataFrame:
    with tgt_engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT quotation_num FROM fact_sales_quotation WHERE order_type_id IS NULL"),
            conn
        )
    print(f"📦 fsq (order_type_id IS NULL): {df.shape}")
    return df

# ---------- TRANSFORM ----------
@op
def build_key_channel(df_plan: pd.DataFrame, df_order_task: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(df_plan, df_order_task, on='quo_num', how='left')

    # parse base/subtype → key_channel
    if "worksend" not in df.columns: df["worksend"] = None
    if "token" not in df.columns: df["token"] = None

    df["key_channel"] = df.apply(parse_channel, axis=1)

    # mapping normalize post-parse
    def _norm_key(x: str) -> str:
        u = str(x).strip().upper()
        if u == "TELE": return "APP TELE"
        if u == "B2B": return "APP B2B"
        if u == "WEB AFF": return "WEB B2C"
        if u == "THAICARE": return "WEB THAICARE"
        if u == "WEB ADMIN": return "WEB ADMIN-B2C"
        return str(x)

    df["key_channel"] = df["key_channel"].apply(_norm_key)
    df["key_channel"] = (df["key_channel"].astype(str)
                         .str.replace("-", " ", regex=False)
                         .str.replace(r"\s+", " ", regex=True)
                         .str.strip())

    # เติม key_channel จาก token: key_channel ว่าง → WEB ถ้ามี token, ไม่งั้น APP
    key_null = df["key_channel"].isna() | (df["key_channel"].astype(str).str.strip() == "")
    token_has = df["token"].notna() & (df["token"].astype(str).str.strip() != "")
    df.loc[key_null & token_has, "key_channel"] = "WEB"
    df.loc[key_null & ~token_has, "key_channel"] = "APP"

    # tidy & rename
    obj_cols = df.select_dtypes(include=["object"]).columns
    df[obj_cols] = df[obj_cols].apply(
        lambda s: s.replace(r"^\s*$", np.nan, regex=True)
                  .replace(r"^\s*(nan|NaN)\s*$", np.nan, regex=True)
    )

    df.rename(columns={
        "quo_num": "quotation_num",
        "type_insure": "type_insurance",
        "type_work": "order_type",
        "type_status": "check_type",
        "worksend": "work_type",
    }, inplace=True)

    # ตามเงื่อนไขเดิม: ถ้า order_type == 1 ให้เป็น NULL
    df.loc[df['order_type'] == 1, 'order_type'] = np.nan

    # ลบคอลัมน์ helper
    df.drop(columns=["token", "chanel_key", "type_key", "app_type"], inplace=True, errors="ignore")

    # แปลง order_type จาก in_advance / check_tax
    df["order_type"] = np.select(
        [
            df.get("in_advance", pd.Series(dtype="Int64")) == 1,
            df.get("check_tax", pd.Series(dtype="Int64")) == 1,
        ],
        ["งานต่ออายุล่วงหน้า", "งานต่อภาษี"],
        default=df["order_type"]
    )

    # cleanup columns
    df.drop(columns=["check_tax", "in_advance"], inplace=True, errors="ignore")
    if "quotation_num" in df.columns:
        df.drop_duplicates(subset=["quotation_num"], keep="first", inplace=True)
    else:
        raise ValueError("DataFrame ไม่มีคอลัมน์ quotation_num สำหรับ upsert")

    # Noneify
    df = df.where(pd.notnull(df), None)

    keep = ["quotation_num", "type_insurance", "order_type", "check_type", "work_type", "key_channel"]
    df = df[keep]
    print(f"🧹 df_key_channel(clean): {df.shape}")
    return df

@op
def attach_order_type_id(df_keys: pd.DataFrame, dim_order: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(
        df_keys,
        dim_order,
        on=['type_insurance', 'order_type', 'check_type', 'work_type', 'key_channel'],
        how="left"
    )
    # เตรียมเฉพาะที่ต้องอัปเดต
    df = df[['quotation_num', 'order_type_id']].drop_duplicates()
    print(f"🔗 df_keys + dim_order_type: {df.shape}")
    return df

@op
def filter_to_missing(df_to_update: pd.DataFrame, fsq_missing: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(df_to_update, fsq_missing, on='quotation_num', how='right')
    # check & save diagnostics (optional)
    if 'order_type_id' in df.columns:
        null_count = df['order_type_id'].isna().sum()
        total_rows = len(df)
        if null_count > 0:
            print(f"⚠️ พบ order_type_id เป็น NULL {null_count} แถว จากทั้งหมด {total_rows} แถว")
            try:
                df[df['order_type_id'].isna()].head(1000).to_excel("order_type_id_null.xlsx", index=False)
                print("💾 เซฟตัวอย่าง NULL → order_type_id_null.xlsx")
            except Exception as _:
                pass
        else:
            print(f"✅ ไม่มี order_type_id เป็น NULL (ทั้งหมด {total_rows} แถว)")
    else:
        print("❌ ไม่มีคอลัมน์ order_type_id ใน DataFrame")

    # clean for update
    df = df[['quotation_num', 'order_type_id']]
    df = df.replace(['None', 'none', 'nan', 'NaN', 'NaT', ''], pd.NA)
    for c in ['quotation_num', 'order_type_id']:
        df[c] = df[c].astype('string').str.strip()
    df = df.dropna(subset=['quotation_num']).drop_duplicates(subset=['quotation_num'])
    print(f"✅ update candidates after FSQ-right-join: {df.shape}")
    return df

# ---------- LOAD (UPDATE) ----------
@op
def update_order_type_id(df_updates: pd.DataFrame) -> None:
    if df_updates.empty:
        print("ℹ️ No rows to update.")
        return

    with tgt_engine.begin() as conn:
        # ลดโอกาส hang/deadlock
        conn.exec_driver_sql("SET lock_timeout = '3s'")
        conn.exec_driver_sql("SET deadlock_timeout = '200ms'")
        conn.exec_driver_sql("SET statement_timeout = '60s'")

        # 1) temp table
        conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_order_type_updates(
                quotation_num text PRIMARY KEY,
                order_type_id text
            ) ON COMMIT DROP
        """)

        # 2) bulk insert
        df_updates.to_sql(
            "tmp_order_type_updates",
            con=conn, if_exists="append", index=False,
            method="multi", chunksize=10_000
        )

        # 3) UPDATE จริง — อัปเดตเฉพาะที่ยังว่างหรือค่าไม่ตรง
        update_sql = text("""
            UPDATE fact_sales_quotation f
            SET order_type_id = t.order_type_id,
                update_at     = NOW()
            FROM tmp_order_type_updates t
            WHERE f.quotation_num = t.quotation_num
              AND (f.order_type_id IS NULL OR f.order_type_id IS DISTINCT FROM t.order_type_id)
        """)

        # 4) retry เมื่อเจอ deadlock
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
        print("✅ Update order_type_id completed.")

# ---------- JOB ----------
@job
def update_fact_sales_quotation_order_type_id():
    update_order_type_id(
        filter_to_missing(
            attach_order_type_id(
                build_key_channel(
                    extract_select_plan(),
                    extract_order_task()
                ),
                extract_dim_order_type()
            ),
            extract_fsq_missing_order_type()
        )
    )

if __name__ == "__main__":
    p = extract_select_plan()
    o = extract_order_task()
    dim = extract_dim_order_type()
    fsq = extract_fsq_missing_order_type()
    keys = build_key_channel(p, o)
    joined = attach_order_type_id(keys, dim)
    updates = filter_to_missing(joined, fsq)
    update_order_type_id(updates)
    print("🎉 completed! order_type_id updated.")
