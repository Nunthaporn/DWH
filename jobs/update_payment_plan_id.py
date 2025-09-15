from dagster import op, job
import os
import re
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =========================
# 🔧 ENV & DB CONNECTIONS
# =========================
load_dotenv()

# MariaDB (source)
src_main_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True, pool_recycle=3600
)
src_task_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_pre_ping=True, pool_recycle=3600
)

# PostgreSQL (target)
tgt_engine = create_engine(
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

NULL_TOKENS = {"", "nan", "none", "null", "undefined", "nat"}

def norm_str(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    return s.mask(s.str.lower().isin(NULL_TOKENS))

# =========================
# 🧲 EXTRACT (MariaDB)
# =========================
@op
def extract_payment_sources() -> pd.DataFrame:
    with src_main_engine.begin() as c_main, src_task_engine.begin() as c_task:
        df_plan = pd.read_sql(
            text("SELECT quo_num, type_insure FROM fin_system_select_plan"),
            c_main
        )
        df_pay = pd.read_sql(
            text("SELECT quo_num, chanel_main, clickbank, chanel, numpay, condition_install FROM fin_system_pay"),
            c_main
        )
        df_order = pd.read_sql(
            text("SELECT quo_num, status_paybill FROM fin_order"),
            c_task
        )

    # แก้ชื่อช่องทางที่เคยเจอในโน้ตบุ๊ก
    df_pay["chanel"] = df_pay["chanel"].replace({"ผ่อนบัตร": "เข้าฟิน"})

    df = pd.merge(df_plan, df_pay, on="quo_num", how="left")
    df = pd.merge(df, df_order, on="quo_num", how="left")
    return df

# =========================
# 🧼 TRANSFORM
# =========================
def _standardize_receiver(row) -> str:
    ch  = str(row.get("chanel", "")).strip().lower()
    chm = str(row.get("chanel_main", "")).strip().lower()
    cb  = str(row.get("clickbank", "")).strip().lower()

    # คงค่า explicit
    if ch in ("เข้าฟิน", "เข้าประกัน"):
        return {"เข้าฟิน": "เข้าฟิน", "เข้าประกัน": "เข้าประกัน"}[ch]

    # กฎเข้าฟินตามเคสที่พบ
    if chm in ("ผ่อนบัตรเครดิต", "ผ่อนบัตร") and (cb in ("creditcard", "") and ch in ("ผ่อนบัตร", "ผ่อนบัตรเครดิต")):
        return "เข้าฟิน"
    if chm == "ตัดบัตรเครดิต" and ((cb in ("", "creditcard")) or cb.startswith("ธนาคาร")) and ch in ("ออนไลน์", "ผ่อนโอน", "ตัดบัตรเครดิต"):
        return "เข้าฟิน"
    if chm == "ผ่อนบัตรเครดิต" and (cb in ("qrcode", "creditcard", "")) and ch == "ออนไลน์":
        return "เข้าฟิน"
    if chm == "ผ่อนบัตรเครดิต" and cb.startswith("ธนาคาร") and ch == "ออนไลน์":
        return "เข้าฟิน"
    if chm == "ผ่อนบัตรเครดิต" and cb == "ธนาคารกรุงไทย" and ch == "ผ่อนบัตร":
        return "เข้าฟิน"

    if chm == "ผ่อนโอน" and cb == "qrcode" and ch == "ผ่อนโอน":
        return "เข้าฟิน"
    if chm == "ผ่อนโอน" and cb.startswith("ธนาคาร") and ch in ("ผ่อนบัตร", "ผ่อนโอน", "ออนไลน์"):
        return "เข้าฟิน"
    if chm == "ผ่อนชำระ" and (cb in ("qrcode", "") or cb.startswith("ธนาคาร")) and ch == "ผ่อนโอน":
        return "เข้าฟิน"
    if chm == "โอนเงิน" and cb.startswith("ธนาคาร") and ch == "ออนไลน์":
        return "เข้าฟิน"

    if chm == "ตัดบัตรเครดิต" and cb == "" and ch == "ตัดบัตรเครดิต":
        return "เข้าฟิน"

    return str(row.get("chanel", "")).strip()

def _determine_payment_channel(row) -> str:
    ch_main = str(row.get("chanel_main", "")).strip().lower()
    cb_raw  = row.get("clickbank")
    cb = str(cb_raw).strip().lower()
    cb_empty = (cb_raw is None) or (cb == "")

    if ch_main in ("ตัดบัตรเครดิต", "ผ่อนบัตร", "ผ่อนบัตรเครดิต", "ผ่อนชำระ"):
        if "qrcode" in cb:
            return "QR Code"
        if "creditcard" in cb:
            return "2C2P"
        return "ตัดบัตรกับฟิน"

    if ch_main in ("โอนเงิน", "ผ่อนโอน"):
        return "QR Code" if "qrcode" in cb else "โอนเงิน"

    if ch_main and cb_empty:
        return row.get("chanel_main") or ""

    if not ch_main and not cb_empty:
        if "qrcode" in cb:
            return "QR Code"
        if "creditcard" in cb:
            return "2C2P"
        return row.get("clickbank") or ""

    if not cb_empty:
        if "qrcode" in cb:
            return "QR Code"
        if "creditcard" in cb:
            return "2C2P"
        return row.get("clickbank") or ""

    return ""

@op
def transform_payment_rows(df_src: pd.DataFrame) -> pd.DataFrame:
    df = df_src.copy()

    # แปลงสตริง 'nan' → NaN
    df = df.applymap(lambda x: np.nan if isinstance(x, str) and x.strip().lower() == "nan" else x)

    # trim text fields
    for col in ["chanel", "chanel_main", "clickbank"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    # มาตรฐานผู้รับเงิน (payment_reciever)
    df["chanel"] = df.apply(_standardize_receiver, axis=1)

    # payment_channel
    df["payment_channel"] = df.apply(_determine_payment_channel, axis=1)

    # ลดคอลัมน์
    df.drop(columns=["chanel_main", "clickbank", "condition_install"], inplace=True, errors="ignore")

    # เปลี่ยนชื่อคอลัมน์ให้ตรง schema
    df.rename(columns={
        "quo_num": "quotation_num",
        "type_insure": "type_insurance",
        "chanel": "payment_reciever",        # (สะกดตาม schema)
        "status_paybill": "payment_type",
        "numpay": "installment_number"
    }, inplace=True)

    # clean ค่า/กรณีพิเศษ
    df["payment_reciever"] = df["payment_reciever"].replace({
        "เข้าประกัน1": "เข้าประกัน",
        "เข้าฟินลิป": "เข้าฟิลลิป"
    })

    # installment_number → int (0 → 1)
    df["installment_number"] = pd.to_numeric(df["installment_number"], errors="coerce").fillna(0).astype(int)
    df["installment_number"] = df["installment_number"].replace({0: 1})

    # คอลัมน์ที่ต้องใช้ downstream
    need_cols = ["quotation_num", "type_insurance", "payment_reciever", "payment_type",
                 "installment_number", "payment_channel"]
    return df[need_cols]

# =========================
# 📖 DIM LOOKUP
# =========================
@op
def fetch_dim_payment_plan() -> pd.DataFrame:
    with tgt_engine.begin() as conn:
        return pd.read_sql(
            text("""
                SELECT payment_plan_id, payment_channel, payment_reciever, payment_type, installment_number
                FROM dim_payment_plan
            """),
            conn
        )

# =========================
# 🔗 JOIN → get payment_plan_id
# =========================
@op
def map_to_payment_plan_id(df_keys: pd.DataFrame, df_dim: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(
        df_keys,
        df_dim,
        on=["payment_channel", "payment_reciever", "payment_type", "installment_number"],
        how="inner"
    )
    return df[["quotation_num", "payment_plan_id"]].drop_duplicates("quotation_num")

# =========================
# 🚀 Restrict + Stage + Update + Drop (รวมใน op เดียว)
# =========================
@op
def upsert_payment_plan_ids(df_pairs: pd.DataFrame) -> int:
    # 1) ดึงเฉพาะ quotation ที่ fact ยังว่างจาก DB
    with tgt_engine.begin() as conn:
        df_missing = pd.read_sql(
            text("SELECT quotation_num FROM fact_sales_quotation WHERE payment_plan_id IS NULL"),
            conn
        )

    need = pd.merge(df_pairs, df_missing, on="quotation_num", how="inner")
    if need.empty:
        print("⚠️ No rows to update.")
        return 0

    # sanitize types
    need["quotation_num"] = norm_str(need["quotation_num"])
    need["payment_plan_id"] = pd.to_numeric(need["payment_plan_id"], errors="coerce").astype("Int64")
    need = need[need["payment_plan_id"].notna()].drop_duplicates(subset=["quotation_num"])

    if need.empty:
        print("⚠️ No resolvable rows after cleaning.")
        return 0

    # 2) Stage temp + 3) Update + 4) Drop ใน transaction เดียว
    with tgt_engine.begin() as conn:
        need.to_sql(
            "dim_payment_plan_temp",
            con=conn,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=20000
        )
        print(f"✅ staged dim_payment_plan_temp: {len(need):,} rows")

        updated = conn.execute(text("""
            UPDATE fact_sales_quotation fsq
            SET payment_plan_id = t.payment_plan_id
            FROM dim_payment_plan_temp t
            WHERE fsq.quotation_num = t.quotation_num
                AND fsq.payment_plan_id IS DISTINCT FROM dc.payment_plan_id;
        """)).rowcount or 0

        conn.execute(text("DROP TABLE IF EXISTS dim_payment_plan_temp"))
        print("🗑️ dropped dim_payment_plan_temp")

    print(f"✅ fact_sales_quotation updated: {updated} rows")
    return updated

# =========================
# 🧱 DAGSTER JOB
# =========================
@job
def update_payment_plan_id_on_fact():
    src   = extract_payment_sources()
    rows  = transform_payment_rows(src)
    dim   = fetch_dim_payment_plan()
    pairs = map_to_payment_plan_id(rows, dim)
    _     = upsert_payment_plan_ids(pairs)

# =========================
# ▶️ Local run (optional)
# =========================
# if __name__ == "__main__":
#     s = extract_payment_sources()
#     r = transform_payment_rows(s)
#     d = fetch_dim_payment_plan()
#     p = map_to_payment_plan_id(r, d)
#     updated = upsert_payment_plan_ids(p)
#     print(f"🎉 done. updated rows = {updated}")
