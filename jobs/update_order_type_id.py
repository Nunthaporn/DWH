from dagster import op, job
import os
import re
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =========================
# 🔧 ENV & DB CONNECTIONS
# =========================
load_dotenv()

# MariaDB (source)
source_engine_main = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True, pool_recycle=3600
)
source_engine_task = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_pre_ping=True, pool_recycle=3600
)

# PostgreSQL (target)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@"
    f"{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c statement_timeout=300000",
    },
    pool_pre_ping=True, pool_recycle=3600
)

# =========================
# 🔧 HELPERS
# =========================
NULL_TOKENS = {"", "nan", "none", "null", "undefined", "nat"}

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

def normalize_str(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    return s.mask(s.str.lower().isin(NULL_TOKENS))

def base_from_type_key(text: str) -> str | None:
    low = _lower(text)
    for pat, label in BASE_MAP:
        if re.search(pat, low):
            return label
    return None

def normalize_special_subtype(raw: str) -> str:
    s = _norm(raw).upper()
    return s.replace("WEB-AFF", "AFF").replace("WEB AFF", "AFF")

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

def derive_subtype(row) -> str | None:
    ch_val = row.get("chanel_key", "")
    app_val = row.get("app_type", "")
    sub = extract_subtype(ch_val) or extract_subtype(app_val)
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

def parse_key_channel(row):
    base = derive_base(row)
    subtype = derive_subtype(row)
    if not subtype:
        subtype = default_subtype_by_product(base, row.get("type_insure", ""))
    if not base and subtype == "VIF":
        base = "WEB"
    # normalize aliases from the original rules
    if subtype:
        s = subtype.strip().upper()
        if s == "TELE":
            return f"APP TELE"
        if s == "B2B":
            return f"APP B2B" if base in (None, "APP") else f"{base} B2B"
        if s == "WEB AFF":
            return "WEB B2C"
        if s == "THAICARE":
            return "WEB THAICARE"
    if base in {"APP", "WEB"} and subtype:
        return f"{base} {subtype}"
    if base and not subtype:
        return base
    if subtype and not base:
        return subtype
    return ""

# =========================
# 🧲 EXTRACT (from MariaDB)
# =========================
@op
def extract_merge_sources() -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT quo_num,type_insure,type_work, type_status , type_key, app_type, chanel_key,token, in_advance, check_tax
        FROM fin_system_select_plan
    """, source_engine_main)

    df1 = pd.read_sql("""
        SELECT quo_num, worksend
        FROM fin_order
    """, source_engine_task)

    out = pd.merge(df, df1, on="quo_num", how="left")
    return out

# =========================
# 🧼 TRANSFORM → Keys for dim_order_type
# =========================
@op
def transform_build_keys(df_merged: pd.DataFrame) -> pd.DataFrame:
    df = df_merged.copy()

    # key_channel
    if "worksend" not in df.columns:
        df["worksend"] = None
    if "token" not in df.columns:
        df["token"] = None

    df["key_channel"] = df.apply(parse_key_channel, axis=1)

    # post normalize
    df["key_channel"] = (
        df["key_channel"].astype(str)
        .str.replace("-", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    # fill from token if key_channel empty
    key_null = df["key_channel"].isna() | (df["key_channel"].astype(str).str.strip() == "")
    token_has = df["token"].notna() & (df["token"].astype(str).str.strip() != "")
    df.loc[key_null & token_has, "key_channel"] = "WEB"
    df.loc[key_null & ~token_has, "key_channel"] = "APP"

    # rename columns to match dim_order_type keys
    df.rename(columns={
        "quo_num": "quotation_num",
        "type_insure": "type_insurance",
        "type_work": "order_type",
        "type_status": "check_type",
        "worksend": "work_type",
    }, inplace=True)

    # rule: if order_type == 1 → NULL
    df.loc[df["order_type"] == 1, "order_type"] = np.nan

    # override order_type by in_advance / check_tax
    df["order_type"] = np.select(
        [
            df["in_advance"] == 1,
            df["check_tax"] == 1
        ],
        [
            "งานต่ออายุล่วงหน้า",
            "งานต่อภาษี"
        ],
        default=df["order_type"]
    )

    # tidy
    df.drop(columns=["type_key", "app_type", "token", "chanel_key"], inplace=True, errors="ignore")
    obj_cols = df.select_dtypes(include=["object"]).columns
    df[obj_cols] = df[obj_cols].apply(
        lambda s: s.replace(r"^\s*$", np.nan, regex=True).replace(r"^\s*(nan|NaN)\s*$", np.nan, regex=True)
    )
    df = df.where(pd.notnull(df), None)

    if "quotation_num" not in df.columns:
        raise ValueError("missing 'quotation_num' for upsert pipeline")
    df = df.drop_duplicates(subset=["quotation_num"], keep="first")

    # keep only keys needed to join with dim_order_type
    cols = ["quotation_num", "type_insurance", "order_type", "check_type", "work_type", "key_channel"]
    return df[cols]

# =========================
# 📖 Load dim_order_type
# =========================
@op
def fetch_dim_order_type() -> pd.DataFrame:
    df = pd.read_sql(
        text("SELECT order_type_id,type_insurance, order_type, check_type, work_type, key_channel FROM dim_order_type"),
        target_engine
    )
    return df

# =========================
# 🔗 Join → get order_type_id per quotation
# =========================
@op
def join_to_dim_order_type(df_keys: pd.DataFrame, df_dim: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(
        df_keys, df_dim,
        on=["type_insurance", "order_type", "check_type", "work_type", "key_channel"],
        how="left"
    )
    out = df[["quotation_num", "order_type_id"]].copy()
    return out

# =========================
# 🧹 Keep only facts missing order_type_id
# =========================
@op
def restrict_to_fact_missing(out_pairs: pd.DataFrame) -> pd.DataFrame:
    facts = pd.read_sql(
        text("SELECT quotation_num FROM fact_sales_quotation WHERE order_type_id IS NULL"),
        target_engine
    )
    out = pd.merge(out_pairs, facts, on="quotation_num", how="inner")
    # keep only rows with a resolved order_type_id
    out = out[out["order_type_id"].notna()].drop_duplicates(subset=["quotation_num"])
    return out[["quotation_num", "order_type_id"]]

# =========================
# 🧼 Stage temp table
# =========================
@op
def stage_dim_order_type_temp(df_map: pd.DataFrame) -> str:
    if df_map.empty:
        df_map = pd.DataFrame(
            {"quotation_num": pd.Series(dtype="string"),
             "order_type_id": pd.Series(dtype="Int64")}
        )
    # sanitize
    df_map = df_map.copy()
    df_map["quotation_num"] = normalize_str(df_map["quotation_num"])
    df_map["order_type_id"] = pd.to_numeric(df_map["order_type_id"], errors="coerce").astype("Int64")

    df_map.to_sql("dim_order_type_temp", target_engine, if_exists="replace", index=False, method="multi", chunksize=20000)
    print(f"✅ staged dim_order_type_temp: {len(df_map):,} rows")
    return "dim_order_type_temp"

# =========================
# 🚀 Update fact with temp (NULL-safe)
# =========================
@op
def update_fact_order_type_id(temp_table_name: str) -> int:
    if not temp_table_name:
        return 0
    update_query = text(f"""
        UPDATE fact_sales_quotation fsq
        SET order_type_id = dc.order_type_id
        FROM {temp_table_name} dc
        WHERE fsq.quotation_num = dc.quotation_num;
    """)
    with target_engine.begin() as conn:
        res = conn.execute(update_query)
        print(f"✅ fact_sales_quotation updated: {res.rowcount} rows")
        return res.rowcount or 0

# =========================
# 🗑️ Drop temp
# =========================
@op
def drop_dim_order_type_temp(temp_table_name: str) -> None:
    if not temp_table_name:
        return
    with target_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))
    print("🗑️ dropped dim_order_type_temp")

# =========================
# 🧱 DAGSTER JOB
# =========================
@job
def update_order_type_id_on_fact():
    merged = extract_merge_sources()
    keys = transform_build_keys(merged)
    dim = fetch_dim_order_type()
    pairs = join_to_dim_order_type(keys, dim)
    needed = restrict_to_fact_missing(pairs)
    temp = stage_dim_order_type_temp(needed)
    _ = update_fact_order_type_id(temp)
    drop_dim_order_type_temp(temp)

# =========================
# ▶️ Local run (optional)
# =========================
# if __name__ == "__main__":
#     m = extract_merge_sources()
#     k = transform_build_keys(m)
#     d = fetch_dim_order_type()
#     p = join_to_dim_order_type(k, d)
#     n = restrict_to_fact_missing(p)
#     t = stage_dim_order_type_temp(n)
#     updated = update_fact_order_type_id(t)
#     drop_dim_order_type_temp(t)
#     print(f"🎉 done. updated rows = {updated}")
