from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Table, MetaData, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta
import re

# ✅ Load environment variables
load_dotenv()

# ✅ Source: MariaDB
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)
source_engine_task  = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task"
)
# ✅ Target: PostgreSQL
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance?connect_timeout=60"
)

@op
def extract_order_type_data():
    # now = datetime.now()

    # start_time = now.replace(minute=0, second=0, microsecond=0)
    # end_time = now.replace(minute=59, second=59, microsecond=999999)

    # start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    # end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    start_str = '2025-08-13'
    end_str = '2025-08-31'

    query_plan = f"""
        SELECT quo_num, type_insure, type_work, type_status, type_key, app_type, chanel_key
        FROM fin_system_select_plan
        WHERE datestart BETWEEN '{start_str}' AND '{end_str}'

    """
    query_order = """
        SELECT quo_num, worksend
        FROM fin_order
    """

    df_plan = pd.read_sql(query_plan, source_engine)
    df_order = pd.read_sql(query_order, source_engine_task)

    df_merged = pd.merge(df_plan, df_order, on='quo_num', how='left')

    print("📦 df_plan:", df_plan.shape)
    print("📦 df_order:", df_order.shape)
    print("📦 df_merged:", df_merged.shape)

    return df_merged

@op
def clean_order_type_data(df: pd.DataFrame):
    import re

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

    # รองรับ subtype ที่ต้องใช้ตามตาราง
    SUBTYPE_SET = {"B2B", "B2C", "TELE", "THAIPOST", "THAICARE"}

    # default subtype ต่อโปรดักต์ เมื่อไม่มี subtype ให้ใช้
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
        """ แปลงรูปแบบพิเศษเช่น WEB-AFF -> AFF """
        s = _norm(raw).upper()
        # รูปแบบที่เจอบ่อย
        s = s.replace("WEB-AFF", "AFF").replace("WEB AFF", "AFF")
        return s

    def extract_subtype(raw: str) -> str | None:
        """
        เลือก subtype จาก raw:
        1) แปลงพิเศษ (WEB-AFF -> AFF)
        2) ค้นหา token ที่อยู่ใน SUBTYPE_SET
        3) ถ้าไม่พบ ให้ใช้ข้อความดิบ (ลบคำ BASE ออก) เป็น subtype
        """
        s = normalize_special_subtype(raw)
        if not s:
            return None

        tokens = re.split(r"[ \-_/]+", s.upper())
        for tok in tokens:
            t = tok.strip()
            if t in SUBTYPE_SET:
                return t

        # ใช้สตริงดิบเป็น subtype (ตัดคำ base ออก)
        s_up = s.upper()
        for _, base_label in BASE_MAP:
            s_up = re.sub(rf"\b{re.escape(base_label)}\b", "", s_up, flags=re.IGNORECASE)
        s_up = re.sub(r"\s+", " ", s_up).strip()
        return s_up if s_up else None

    def derive_base(row) -> str | None:
        # ✅ ใช้ type_key เป็นหลักเท่านั้น
        return base_from_type_key(row.get("type_key", ""))

    def derive_subtype(row) -> str | None:
        # ✅ เอาจาก chanel_key ก่อน ถ้าไม่มีค่อยใช้ app_type
        ch_val = row.get("chanel_key", "")
        app_val = row.get("app_type", "")
        sub = extract_subtype(ch_val)
        if sub:
            return sub
        sub = extract_subtype(app_val)
        if sub:
            return sub

        # fallback: พบคำ vif/ตรอ ให้เป็น VIF (เข้าชุด SUBTYPE_SET ไหม? ถ้าอยากได้ก็เพิ่ม)
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
            # map โดยใช้คีย์เป็น lower เพื่อกันเคสสะกด
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

        # ถ้ายังไม่มี subtype ให้ใช้ default ต่อโปรดักต์
        if not subtype:
            subtype = default_subtype_by_product(base, row.get("type_insure", ""))

        # ถ้ายังไม่มี base แต่ subtype == VIF -> default base = WEB (คงไว้)
        if not base and subtype == "VIF":
            base = "WEB"

        # ประกอบผลลัพธ์ (ตามตาราง: APP/WEB + subtype เมื่อมี)
        if base in {"APP", "WEB"} and subtype:
            return f"{base} {subtype}"
        if base and not subtype:
            return base
        if subtype and not base:
            return subtype
        return ""

    # ---------- build key_channel ----------
    if "worksend" not in df.columns:
        df["worksend"] = None

    df["key_channel"] = df.apply(parse_channel, axis=1)

    # แก้เคสที่ได้ "TELE" เฉย ๆ ให้เป็น "APP TELE"
    df["key_channel"] = df["key_channel"].apply(lambda x: "APP TELE" if str(x).strip().upper() == "TELE" else x)
    df["key_channel"] = df["key_channel"].apply(lambda x: "APP B2B" if str(x).strip().upper() == "B2B" else x)
    df["key_channel"] = df["key_channel"].apply(lambda x: "WEB B2C" if str(x).strip().upper() == "WEB AFF" else x)
    df["key_channel"] = df["key_channel"].apply(lambda x: "WEB THAICARE" if str(x).strip().upper() == "THAICARE" else x)
    df["key_channel"] = df["key_channel"].apply(lambda x: "WEB ADMIN-B2C" if str(x).strip().upper() == "WEB ADMIN" else x)

    # ลบขีด (-) ทั้งหมดใน key_channel
    df["key_channel"] = df["key_channel"].astype(str).str.replace("-", " ", regex=False).str.replace(r"\s+", " ", regex=True).str.strip()

    # ---------- tidy & rename ----------
    obj_cols = df.select_dtypes(include=["object"]).columns
    df[obj_cols] = df[obj_cols].apply(
        lambda s: s.replace(r"^\s*$", np.nan, regex=True)
                  .replace(r"^\s*(nan|NaN)\s*$", np.nan, regex=True)
    )

    # ลบตัวช่วยหลังคำนวณแล้ว
    df.drop(columns=["type_key", "app_type"], inplace=True, errors="ignore")

    df.loc[df['order_type'] == 1, 'order_type'] = np.nan

    df.rename(columns={
        "quo_num": "quotation_num",
        "type_insure": "type_insurance",
        "type_work": "order_type",
        "type_status": "check_type",
        "worksend": "work_type",
    }, inplace=True)

    if "quotation_num" in df.columns:
        df.drop_duplicates(subset=["quotation_num"], keep="first", inplace=True)

    df = df.where(pd.notnull(df), None)
    print("📊 Cleaning completed (key_channel mapped)")
    return df

@op
def load_order_type_data(df: pd.DataFrame):
    table_name = 'dim_order_type'
    
    # ตรวจสอบโครงสร้างตารางปัจจุบัน
    with target_engine.connect() as conn:
        # ตรวจสอบว่ามีคอลัมน์ quotation_num หรือไม่
        result = conn.execute(text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND column_name = 'quotation_num'
        """))
        has_quotation_num = result.fetchone() is not None
        
        # ตรวจสอบ unique constraint ที่มีอยู่จริง
        result = conn.execute(text(f"""
            SELECT conname, contype
            FROM pg_constraint
            WHERE conrelid = '{table_name}'::regclass
            AND contype = 'u'
        """))
        unique_constraints = result.fetchall()
        
        # ตรวจสอบ primary key
        result = conn.execute(text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND column_name = 'order_type_id'
        """))
        has_order_type_id = result.fetchone() is not None

    print(f"🔍 Debug: has_quotation_num = {has_quotation_num}")
    print(f"🔍 Debug: unique_constraints = {unique_constraints}")
    print(f"🔍 Debug: has_order_type_id = {has_order_type_id}")

    if has_quotation_num and unique_constraints:
        # กรณีที่ยังมี quotation_num column และมี unique constraint (กรณีเก่า)
        pk_column = 'quotation_num'
        
        # ตรวจสอบว่ามี unique constraint บน quotation_num หรือไม่
        has_quotation_unique = any('quotation_num' in str(constraint) for constraint in unique_constraints)
        
        if not has_quotation_unique:
            # ถ้าไม่มี unique constraint บน quotation_num ให้สร้างใหม่
            with target_engine.connect() as conn:
                conn.execute(text(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_quotation_num') THEN
                            ALTER TABLE {table_name} ADD CONSTRAINT unique_quotation_num UNIQUE ({pk_column});
                        END IF;
                    END $$;
                """))
                conn.commit()

        df = df[~df[pk_column].duplicated(keep='first')].copy()

        # ✅ วันปัจจุบัน (เริ่มต้นเวลา 00:00:00)
        today_str = datetime.now().strftime('%Y-%m-%d')

        # ✅ Load เฉพาะข้อมูลวันนี้จาก PostgreSQL
        with target_engine.connect() as conn:
            df_existing = pd.read_sql(
                f"SELECT * FROM {table_name} WHERE update_at >= '{today_str}'",
                conn
            )

        df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

        new_ids = set(df[pk_column]) - set(df_existing[pk_column])
        df_to_insert = df[df[pk_column].isin(new_ids)].copy()

        common_ids = set(df[pk_column]) & set(df_existing[pk_column])
        df_common_new = df[df[pk_column].isin(common_ids)].copy()
        df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

        merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

        exclude_columns = [pk_column, 'order_type_id', 'create_at', 'update_at']

        # ✅ คำนวณ column ที่เหมือนกันทั้ง df และ df_existing เท่านั้น
        all_columns = set(df_common_new.columns) & set(df_common_old.columns)
        compare_cols = [
            col for col in all_columns
            if col not in exclude_columns
            and f"{col}_new" in merged.columns
            and f"{col}_old" in merged.columns
        ]

        def is_different(row):
            for col in compare_cols:
                val_new = row.get(f"{col}_new")
                val_old = row.get(f"{col}_old")
                if pd.isna(val_new) and pd.isna(val_old):
                    continue
                if val_new != val_old:
                    return True
            return False

        # Filter rows that have differences
        df_diff = merged[merged.apply(is_different, axis=1)].copy()

        if not df_diff.empty and compare_cols:
            update_cols = [f"{col}_new" for col in compare_cols]
            all_cols = [pk_column] + update_cols

            # ✅ เช็คให้ชัวร์ว่าคอลัมน์ที่เลือกมีจริง
            existing_cols = [c for c in all_cols if c in df_diff.columns]
            
            if len(existing_cols) > 1:  # ต้องมี pk_column และอย่างน้อย 1 คอลัมน์อื่น
                df_diff_renamed = df_diff.loc[:, existing_cols].copy()
                # เปลี่ยนชื่อ column ให้ตรงกับตารางจริง
                new_col_names = [pk_column] + [col.replace('_new', '') for col in existing_cols if col != pk_column]
                df_diff_renamed.columns = new_col_names
            else:
                df_diff_renamed = pd.DataFrame()
        else:
            df_diff_renamed = pd.DataFrame()

        print(f"🆕 Insert: {len(df_to_insert)} rows")
        print(f"🔄 Update: {len(df_diff_renamed)} rows")

        metadata = Table(table_name, MetaData(), autoload_with=target_engine)

        # Insert only the new records
        if not df_to_insert.empty:
            df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
            dropped = len(df_to_insert) - len(df_to_insert_valid)
            if dropped > 0:
                print(f"⚠️ Skipped {dropped}")
            if not df_to_insert_valid.empty:
                with target_engine.begin() as conn:
                    conn.execute(metadata.insert(), df_to_insert_valid.to_dict(orient='records'))

        # Update only the records where there is a change
        if not df_diff_renamed.empty and compare_cols:
            with target_engine.begin() as conn:
                for record in df_diff_renamed.to_dict(orient='records'):
                    stmt = pg_insert(metadata).values(**record)
                    update_columns = {
                        c.name: stmt.excluded[c.name]
                        for c in metadata.columns
                        if c.name not in [pk_column, 'order_type_id', 'create_at', 'update_at']
                    }
                    # update_at ให้เป็นเวลาปัจจุบัน
                    update_columns['update_at'] = datetime.now()
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[pk_column],
                        set_=update_columns
                    )
                    conn.execute(stmt)

    else:
        # กรณีที่ไม่มี quotation_num column หรือไม่มี unique constraint (กรณีใหม่) - ใช้ INSERT แบบปกติ
        print("📝 No quotation_num column or unique constraint found, using simple INSERT")
        
        metadata = Table(table_name, MetaData(), autoload_with=target_engine)
        
        # เพิ่ม timestamp columns ถ้าไม่มี
        if 'create_at' not in df.columns:
            df['create_at'] = pd.Timestamp.now()
        if 'update_at' not in df.columns:
            df['update_at'] = pd.Timestamp.now()
        
        # ตรวจสอบว่าคอลัมน์ใน DataFrame ตรงกับตารางหรือไม่
        table_columns = [c.name for c in metadata.columns]
        df_columns = [col for col in df.columns if col in table_columns]
        df_filtered = df[df_columns].copy()
        
        # ลบ duplicates ถ้ามี
        df_filtered = df_filtered.drop_duplicates()
        
        print(f"🆕 Insert: {len(df_filtered)} rows")
        
        # Insert records
        if not df_filtered.empty:
            with target_engine.begin() as conn:
                conn.execute(metadata.insert(), df_filtered.to_dict(orient='records'))

    print("✅ Insert/update completed.")

@job
def dim_order_type_etl():
    load_order_type_data(clean_order_type_data(extract_order_type_data()))

if __name__ == "__main__":
    df_row = extract_order_type_data()
    # print("✅ Extracted logs:", df_row.shape)

    df_clean = clean_order_type_data((df_row))
    # print("✅ Cleaned columns:", df_clean.columns)

    # output_path = "dim_order_type.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_order_type_data(df_clean)
    print("🎉 completed! Data upserted to dim_order_type.")