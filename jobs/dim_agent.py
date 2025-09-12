from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table, func, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta

# ============ ENV & ENGINES ============
load_dotenv()

# MariaDB (source)
source_engine = create_engine(
    "mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}".format(
        user=os.getenv("DB_USER"),
        pwd=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        db="fininsurance",
    ),
    pool_pre_ping=True,
)

# PostgreSQL (target)
target_engine = create_engine(
    "postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}".format(
        user=os.getenv("DB_USER_test"),
        pwd=os.getenv("DB_PASSWORD_test"),
        host=os.getenv("DB_HOST_test"),
        port=os.getenv("DB_PORT_test"),
        db="fininsurance",
    ),
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c statement_timeout=300000",
    },
    pool_pre_ping=True,
)

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# ============ EXTRACT (today only) ============
@op
def extract_agent_data():
    # คำนวณช่วงเวลา "วันนี้" ตามเวลาไทย
    if ZoneInfo:
        tz = ZoneInfo("Asia/Bangkok")
        now_th = datetime.now(tz)
        start_th = now_th.replace(hour=0, minute=0, second=0, microsecond=0)
        end_th = start_th + timedelta(days=1)
        start = start_th.replace(tzinfo=None)  # ตัด timezone ก่อนส่งไป DB
        end = end_th.replace(tzinfo=None)
    else:
        # fallback: สมมติ UTC+7
        now = datetime.utcnow() + timedelta(hours=7)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)

    print(f"⏱️ Querying between {start} and {end}")

    query_main = text("""
        SELECT
            cuscode, name, rank,
            user_registered,
            status, fin_new_group, fin_new_mem,
            type_agent, typebuy, user_email, name_store, address, city, district,
            province, province_cur, area_cur, postcode, tel, date_active, 'display_name',
            headteam, status_vip
        FROM wp_users
        WHERE
            (
              (cuscode = 'WEB-T2R')  -- ✅ whitelist
              OR (
                user_login NOT IN ('FINTEST-01', 'FIN-TestApp', 'adminmag_fin', 'FNG00-00001')
                AND name NOT LIKE '%%ทดสอบ%%'
                AND name NOT LIKE '%%tes%%'
                AND name NOT LIKE '%%test%%'
                AND name NOT LIKE '%%เทสระบบ%%'
                AND name NOT LIKE '%%Tes ระบบ%%'
                AND name NOT LIKE '%%ทด่ท%%'
                AND name NOT LIKE '%%ทด สอบ%%'
                AND name NOT LIKE '%%ปัญญวัฒน์ โพธิ์ศรีทอง%%'
                AND name NOT LIKE '%%เอกศิษฎ์ เจริญธันยบูรณ์%%'
                AND cuscode NOT LIKE '%%FIN-TestApp%%'
                AND cuscode NOT LIKE '%%FIN-Tester1%%'
                AND cuscode NOT LIKE '%%FIN-Tester2%%'
                AND display_name NOT LIKE '%%ทดสอบ%%'
              )
            )
            -- ✅ กรองข้อมูลวันนี้ (เวลาไทย)
            AND (
                (user_registered IS NOT NULL AND user_registered >= :start AND user_registered < :end)
                OR
                (date_active   IS NOT NULL AND date_active   >= :start AND date_active   < :end)
            )
    """)

    df_main = pd.read_sql(query_main, source_engine, params={"start": start, "end": end})

    # career (left join)
    query_career = text("SELECT cuscode, career FROM policy_register")
    df_career = pd.read_sql(query_career, source_engine)

    print("📦 df_main:", df_main.shape)
    print("📦 df_career:", df_career.shape)

    df = pd.merge(df_main, df_career, on="cuscode", how="left")
    print("🔎 After merge: rows =", len(df))

    if "career" in df.columns:
        df["career"] = df["career"].astype(str).str.strip()

    return df

# ============ CLEAN ============
@op
def clean_agent_data(df: pd.DataFrame):
    df = df.copy()

    # ------- normalize columns that we use -------
    for col in ["cuscode", "status", "fin_new_group", "fin_new_mem"]:
        if col not in df.columns:
            df[col] = None
    df["cuscode"] = df["cuscode"].astype(str).str.strip()

    # ------- defect status (fix .str.strip()) -------
    status_s = df["status"].astype(str).str.strip().str.lower()
    had_suffix = df["cuscode"].str.contains(r"-defect$", case=False, na=False)
    base_id = df["cuscode"].str.replace(r"-defect$", "", regex=True)
    df["cuscode"] = np.where(had_suffix, base_id + "-defect", base_id)

    is_defect_after = df["cuscode"].str.contains(r"-defect$", case=False, na=False) | status_s.eq("defect")
    df["defect_status"] = np.where(is_defect_after, "defect", None)

    # ------- build agent_region (join group+mem) -------
    def _join(a, b):
        a = "" if pd.isna(a) else str(a).strip()
        b = "" if pd.isna(b) else str(b).strip()
        if a and b:
            return a if a == b else f"{a} + {b}"
        return a or b

    df["agent_region"] = df.apply(lambda r: _join(r["fin_new_group"], r["fin_new_mem"]), axis=1)
    df = df[~df["agent_region"].str.contains("TEST", case=False, na=False)]

    # ------- filter TEST EXACT only (both group & mem == TEST) with whitelist -------
    whitelist = {"WEB-T2R", "WEB-T2R-DEFECT", "Admin-VIF"}  # เพิ่ม Admin-VIF
    cus_up = df["cuscode"].str.upper()
    g = df["fin_new_group"].astype(str).str.strip().str.upper()
    m = df["fin_new_mem"].astype(str).str.strip().str.upper()
    mask_test_exact = g.eq("TEST") & m.eq("TEST")
    mask_keep = cus_up.isin(whitelist)
    df = df[~(mask_test_exact & ~mask_keep)].copy()

    def clean_region(region_str: str) -> str:
        if not isinstance(region_str, str):
            return ""
        # แยกด้วย '+'
        parts = [p.strip() for p in region_str.split('+')]
        # ลบตัวเลขท้ายออก
        cleaned = [re.sub(r"\d+$", "", p).strip() for p in parts]
        # เก็บ unique ตามลำดับที่เจอ
        unique = []
        for c in cleaned:
            if c not in unique:
                unique.append(c)
        # รวมกลับด้วย " + "
        return " + ".join(unique)

    df["agent_main_region"] = df["agent_region"].apply(clean_region)

    df = df.drop(columns=["fin_new_group", "fin_new_mem", "display_name"], errors="ignore")

    # ------- rename columns -------
    rename_map = {
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
    df.rename(columns=rename_map, inplace=True)

    # ✅ ลบ space ด้านหน้าในทุกคอลัมน์ข้อความ
    def clean_leading_spaces(text):
        if pd.isna(text) or text == '':
            return None
        text_str = str(text).strip()
        cleaned_text = re.sub(r'^\s+', '', text_str)
        return cleaned_text if cleaned_text != '' else None

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(clean_leading_spaces)

    # ------- finalize defect_status & drop old status -------
    if "status" in df.columns:
        status_s2 = df["status"].astype(str).str.strip().str.lower()
        df["defect_status"] = np.where(
            (df["defect_status"] == "defect") | status_s2.eq("defect"),
            "defect",
            None,
        )
        df.drop(columns=["status"], inplace=True)

    # ------- light cleaning (no dedup to keep counts!) -------
    # experience flip then map to yes/no
    df["is_experienced"] = df["is_experienced"].apply(
        lambda x: "เคยขาย" if str(x).strip().lower() == "ไม่เคยขาย" else "ไม่เคยขาย"
    )
    df["is_experienced"] = df["is_experienced"].apply(
        lambda x: "no" if str(x).strip().lower() == "ไม่เคยขาย" else "yes"
    )

    valid_rank = {str(i) for i in range(1, 11)}
    if "agent_rank" in df.columns:
        df.loc[~df["agent_rank"].isin(valid_rank), "agent_rank"] = np.nan

    # address trim
    if "agent_address" in df.columns:
        df["agent_address"] = df["agent_address"].apply(
            lambda addr: re.sub(r"(เลขที่|หมู่ที่|หมู่บ้าน|ซอย|ถนน)[\s\-]*", "", str(addr)).strip()
            if pd.notna(addr) else None
        )

    # ✅ ทำความสะอาด agent_address (สระ/อักขระพิเศษ)
    def clean_address(address):
        if pd.isna(address) or address == '':
            return None
        address_str = str(address).strip()
        cleaned_address = re.sub(r'^[\u0E30-\u0E3A\u0E47-\u0E4E]+', '', address_str)
        cleaned_address = re.sub(r'[-:.,]', '', cleaned_address)
        cleaned_address = re.sub(r'\s+', ' ', cleaned_address).strip()
        return cleaned_address

    df["agent_address"] = df["agent_address"].apply(clean_address)

    # phone digits only
    if "mobile_number" in df.columns:
        df["mobile_number"] = df["mobile_number"].astype(str).str.replace(r"[^0-9]", "", regex=True)

    # email clean
    def clean_email(email):
        if pd.isna(email) or email == "":
            return None
        s = str(email).strip()
        if re.findall(r"[ก-๙]", s):
            return None
        return s.lower() if re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", s) else None

    if "agent_email" in df.columns:
        df["agent_email"] = df["agent_email"].apply(clean_email)

    test_names = [
        'ADVISORY TEST', 'ADVISORY TESTER', 'Sawitri Test K', 'Tes ระบบ', 'test', 'Test', 'Test Fin', 'Test Northeast',
        'test t', 'test test', 'Test Test', 'test text', 'test tt', 'Test สมัครลูกทีม ฟิลลิป', 'testdddddd ddddddd',
        'testsdt tstset', 'TestSMS Test', 'TestTester', 'Testสุพจน์ วงค์แก้ว', 'ทดสอบวันที่', 'ทดสอบ สมัคร', 'ทดสอบเอกศิษฏ์',
        'ทดสอบระบบฟิน', 'ทดสอบ ทดสอบ', 'ทดสอบสมัคร', 'ทดสอบเลขบัญชี', 'ทดสอบระบบ', 'ทดสอบแม็ค', 'ทดสอบ PDPA',
        'ทดสอบระบบ แอปพลิเคชั่น', 'ทดสอบ ลูกค้าไม่หมดอายุ', 'ทดสอบ ลูกค้าไม่หมดอายุผ่อน', 'ทดสอบ ลูกค้าหมดอายุ',
        'ทดสอบ สมาชิกใหม่', 'ทดสอบ บัตรปชช', 'ทดสอบ หฤษฎ์', 'ทดสอบ ทดสอบ', 'ทดสอบ BCOHM', 'ทดสอบ แบบทดสอบ',
        'ทดสอบ เชิญ', 'ทดสอบแชร์', 'ทดสอบระบบ นามสกุลดี', 'ทดสอบจากไอที ไม่ต้องติดต่อกลับ', 'ทดสอบสมัครแบบแชร์',
        'นายศิวกร รุ่งเรืองทดสอบระบบ', 'เอกศิษฏ์ เจริญธันยบูรณ์ ทดสอบระบบ', 'ทดสอบ ระบบ', 'ทดสอบ สอบทด', 'แม็คทดสอบ',
        'ทดสอบสมัคร ทดสอบ', 'jamesit ทดสอบ', 'ทดสอบจริงจัง ทดสอบจริงจัง', 'ทดสอบ FinCare ทดสอบ', 'ทดสอบ นายหน้ามีผู้แนะนำ',
        'ทดสอบ นายหน้าเก่ามีผู้แนะนำ', 'ทดสอบระบบ ทดสอบระบบ', 'ทดสอบระบบ จริงจริงนะ', 'ทดสอบ ระบบอีกครั้ง',
        'ทดสอบจริงจัง ทดสอบจริงจัง', 'ทดสอบ ทดสอบappsale', 'ทดสอบ ทดสอบสมัคร', 'ทดสอบ ทดสอบ', 'ปัญญา เกรียงไกร เทสระบบ',
        'ปัญญา เกรียงไกร (เทสระบบ)', 'เทสๆ'
    ]

    def clean_agent_name(name):
        if pd.isna(name) or name == "":
            return name
        s = str(name).strip()
        s = re.sub(r'^[ิีึืุู่๊๋่้๊๋,._ _;???.]+', '', s)
        if any(test_name in s for test_name in test_names):
            return None
        return s

    if "agent_name" in df.columns:
        df["agent_name"] = df["agent_name"].apply(clean_agent_name)

    # thai-only fields
    def check_thai_text(text):
        if pd.isna(text) or text == "":
            return None
        t = re.sub(r"^[\s\]\*]+", "", str(text).strip())
        if re.match(r"^\d+$", t) or re.match(r"^[A-Za-z\s]+$", t):
            return None
        return t if re.findall(r"[ก-๙]", t) else None

    for col in ["subdistrict", "district", "province", "current_province", "current_area"]:
        if col in df.columns:
            df[col] = df[col].apply(check_thai_text)

    # zipcode
    if "zipcode" in df.columns:
        df["zipcode"] = df["zipcode"].apply(
            lambda z: str(z).strip() if pd.notna(z) and re.match(r"^\d{5}$", str(z).strip()) else None
        )

    # generic trim (skip date fields)
    date_cols = {"date_active"}
    for col in df.columns:
        if col in {"agent_id"} | date_cols:
            continue
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: (re.sub(r"^\s+", "", str(x).strip()) or None) if pd.notna(x) and x != "" else None
            )

    # hire_date -> Int YYYYMMDD
    if "hire_date" in df.columns:
        dt = pd.to_datetime(df["hire_date"], errors="coerce")
        df["hire_date"] = dt.dt.strftime("%Y%m%d").where(dt.notnull(), None)
        df["hire_date"] = df["hire_date"].astype("Int64")

    # date_active -> python datetime or None
    if "date_active" in df.columns:
        dt = pd.to_datetime(df["date_active"], errors="coerce")
        try:
            dt = dt.dt.tz_localize(None)
        except Exception:
            pass
        df["date_active"] = [
            (v.to_pydatetime() if isinstance(v, pd.Timestamp) and pd.notna(v)
             else (v if isinstance(v, datetime) else None))
            for v in dt
        ]

    df = df.replace(["None", "none", "nan", "NaN", "NaT", ""], np.nan)

    # ===== ✅ NEW: keep only -defect row when both exist =====
    # สร้าง base_id (ตัด -defect ที่ท้าย) และ flag แถวที่เป็น defect
    df["base_id"] = df["agent_id"].str.replace(r"-defect$", "", regex=True)
    df["is_defect"] = df["agent_id"].str.contains(r"-defect$", case=False, na=False) | df["defect_status"].eq("defect")

    # จัดเรียงให้ non-defect มาก่อน defect แล้ว drop duplicates โดยเก็บตัวท้าย (defect) ไว้
    df = (
        df.sort_values(["base_id", "is_defect"])
          .drop_duplicates(subset=["base_id"], keep="last")
          .drop(columns=["base_id", "is_defect"])
    )
    # ================================================

    print("✅ cleaned rows:", len(df))
    return df

# ============ LOAD ============
@op
def load_to_wh(df: pd.DataFrame):
    table_name = "dim_agent"
    pk_column = "agent_id"

    df = df.where(pd.notnull(df), None)

    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT {pk_column} FROM {table_name}", conn)

    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()

    print(f"🆕 Insert candidates: {len(df_to_insert)}")
    print(f"🔄 Update candidates: {len(df_common_new)}")

    metadata_table = Table(table_name, MetaData(), autoload_with=target_engine)

    def chunk_dataframe(dfx, chunk_size=500):
        for i in range(0, len(dfx), chunk_size):
            yield dfx.iloc[i:i + chunk_size]

    def sanitize_batch_remove_date_active(df_batch: pd.DataFrame) -> list[dict]:
        df_batch = df_batch.where(pd.notnull(df_batch), None)
        recs = []
        for rec in df_batch.to_dict(orient="records"):
            if "date_active" in rec:
                del rec["date_active"]
            recs.append(rec)
        return recs

    # ---------- UPSERT NEW (insert เท่านั้นจริง ๆ จะไม่ conflict อยู่แล้ว) ----------
    if not df_to_insert.empty:
        with target_engine.begin() as conn:
            for batch_df in chunk_dataframe(df_to_insert):
                records = sanitize_batch_remove_date_active(batch_df.copy())
                if not records:
                    continue
                stmt = pg_insert(metadata_table).values(records)
                cols = [c.name for c in metadata_table.columns]

                # ไม่อัปเดต pk และ date_active ในรอบนี้
                updatable_cols = [c for c in cols if c not in [pk_column, "date_active"]]

                update_columns = {c: stmt.excluded[c] for c in updatable_cols}

                # ให้ update_at = NOW() เมื่อเกิดการ UPDATE จาก conflict (กรณีมี row อยู่แล้ว)
                if "update_at" in cols:
                    update_columns["update_at"] = func.now()

                # เงื่อนไข: อัปเดตก็ต่อเมื่อมีคอลัมน์ไหน "เปลี่ยนจริง"
                # excluded.col IS DISTINCT FROM table.col (null-safe compare)
                change_exprs = [
                    stmt.excluded[c].is_distinct_from(getattr(metadata_table.c, c))
                    for c in updatable_cols if c not in ["create_at", "update_at"]
                ]
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns,
                    where=or_(*change_exprs) if change_exprs else None
                )
                conn.execute(stmt)

    # ---------- UPSERT UPDATE (เฉพาะ id ที่มีอยู่: เปลี่ยนแปลงจริงเท่านั้นจึง update + touch update_at) ----------
    if not df_common_new.empty:
        with target_engine.begin() as conn:
            for batch_df in chunk_dataframe(df_common_new):
                records = sanitize_batch_remove_date_active(batch_df.copy())
                if not records:
                    continue
                stmt = pg_insert(metadata_table).values(records)
                cols = [c.name for c in metadata_table.columns]

                # กันคอลัมน์ที่ไม่อยากให้ override ตรง ๆ
                updatable_cols = [
                    c for c in cols
                    if c not in [pk_column, "id_contact", "create_at", "update_at", "date_active"]
                ]

                update_columns = {c: stmt.excluded[c] for c in updatable_cols}

                # ให้ update_at = NOW() เฉพาะเมื่อเกิดการ UPDATE (ซึ่งมี where ตรวจความเปลี่ยนแปลง)
                if "update_at" in cols:
                    update_columns["update_at"] = func.now()

                # เงื่อนไขอัปเดตเฉพาะเมื่อมีค่าต่างจากเดิมจริง ๆ
                change_exprs = [
                    stmt.excluded[c].is_distinct_from(getattr(metadata_table.c, c))
                    for c in updatable_cols
                ]

                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns,
                    where=or_(*change_exprs) if change_exprs else None
                )
                conn.execute(stmt)

    print("✅ Insert/update completed (date_active skipped in this step).")


# ============ BACKFILL ============

@op
def backfill_date_active(df: pd.DataFrame):
    table_name = "dim_agent"
    pk = "agent_id"
    if "date_active" not in df.columns:
        print("⚠️ Input df has no date_active column — skip backfill")
        return

    s = pd.to_datetime(df["date_active"], errors="coerce")
    try:
        s = s.dt.tz_localize(None)
    except Exception:
        pass

    df_dates = pd.DataFrame({pk: df[pk].astype(str).str.strip(), "date_active": s})
    df_dates = df_dates[df_dates[pk].astype(bool)].copy()

    # keep one per pk (prefer not-null most recent)
    df_dates["__rank"] = df_dates["date_active"].notna().astype(int)
    df_dates = df_dates.sort_values([pk, "__rank", "date_active"], ascending=[True, False, False])
    df_dates = df_dates.drop(columns="__rank").drop_duplicates(subset=[pk], keep="first")

    def _coerce_py_datetime(v):
        try:
            if pd.isna(v): return None
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
            if v.strip().lower() == "nat":
                return None
            try:
                return pd.Timestamp(v).to_pydatetime()
            except Exception:
                return None
        if isinstance(v, datetime):
            return v
        return None

    df_dates["date_active"] = df_dates["date_active"].apply(_coerce_py_datetime)
    df_dates = df_dates.replace({pd.NaT: None, "NaT": None})

    metadata_table = Table(table_name, MetaData(), autoload_with=target_engine)

    def chunk(dfx, n=500):
        for i in range(0, len(dfx), n):
            yield dfx.iloc[i:i+n]

    total = 0
    with target_engine.begin() as conn:
        for b in chunk(df_dates[[pk, "date_active"]]):
            records = []
            for rec in b.to_dict(orient="records"):
                rec["date_active"] = _coerce_py_datetime(rec.get("date_active"))
                records.append(rec)
            if not records:
                continue

            stmt = pg_insert(metadata_table).values(records)
            cols = [c.name for c in metadata_table.columns]

            set_map = {"date_active": stmt.excluded["date_active"]}
            if "update_at" in cols:
                # touch update_at เฉพาะตอนที่ date_active เปลี่ยนจริง
                set_map["update_at"] = func.now()

            # อัปเดตก็ต่อเมื่อ date_active เปลี่ยนจริงเท่านั้น
            where_change = stmt.excluded["date_active"].is_distinct_from(metadata_table.c.date_active)

            stmt = stmt.on_conflict_do_update(
                index_elements=[pk],
                set_=set_map,
                where=where_change
            )
            conn.execute(stmt)
            total += len(records)

    print(f"✅ Backfilled date_active for {total} agents (update_at touched only when changed)")

# ============ WRAPPER ============
@op
def clean_null_values_op(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(["None", "none", "nan", "NaN", "NaT", ""], np.nan)

@job
def dim_agent_etl():
    df_raw = extract_agent_data()
    df_clean = clean_agent_data(clean_null_values_op(df_raw))
    load_to_wh(df_clean)
    backfill_date_active(df_clean)

# if __name__ == "__main__":
#     df_raw = extract_agent_data()
#     print("✅ Extracted logs:", df_raw.shape)

#     df_clean = clean_agent_data(df_raw)
#     print("✅ Cleaned rows:", len(df_clean))

#     df_clean = clean_null_values_op(df_clean)

#     df_clean.to_excel("dim_agent1.xlsx", index=False)
#     print("💾 Saved to dim_agent.xlsx")

#     # load_to_wh(df_clean)
#     # backfill_date_active(df_clean)
#     print("🎉 completed!")


