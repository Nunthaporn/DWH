from dagster import op, job, schedule
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy import or_, func
import re

# ---- timezone helper ----
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None

def _today_range_th():
    """Return naive datetimes [start, end) for 'today' in Asia/Bangkok."""
    if ZoneInfo:
        tz = ZoneInfo("Asia/Bangkok")
        now_th = datetime.now(tz)
        start_th = now_th.replace(hour=0, minute=0, second=0, microsecond=0)
        end_th = start_th + timedelta(days=1)
        return start_th.replace(tzinfo=None), end_th.replace(tzinfo=None)
    # fallback: UTC+7
    now = datetime.utcnow() + timedelta(hours=7)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

# -------- Helpers (ปรับใหม่) --------

def _strip_commas_and_spaces(series: pd.Series) -> pd.Series:
    """ลบ comma และ space เฉพาะค่า string; ไม่บังคับ astype(str) ทั้งคอลัมน์"""
    def _clean_one(x):
        if isinstance(x, str):
            return x.replace(',', '').replace(' ', '')
        return x
    return series.map(_clean_one)

def to_nullable_float(series: pd.Series) -> pd.Series:
    """
    แปลงซีรีส์เป็นตัวเลขแบบ nullable 'Float64':
    - แก้คอมม่า/ช่องว่างก่อน
    - coerce ให้เป็น NaN แล้ว cast เป็น Float64 (จะได้ pd.NA แทน NaN)
    """
    s = _strip_commas_and_spaces(series)
    s = pd.to_numeric(s, errors="coerce")
    return s.astype('Float64')

def to_nullable_int(series: pd.Series) -> pd.Series:
    s = _strip_commas_and_spaces(series)
    s = pd.to_numeric(s, errors="coerce")
    return s.astype('Int64')

def has_comma_value(series: pd.Series) -> int:
    """นับเฉพาะค่า string ที่ยังมี comma; ไม่ใช้ astype(str)"""
    return series.map(lambda x: isinstance(x, str) and (',' in x)).sum()

def clean_object_nans(df: pd.DataFrame) -> pd.DataFrame:
    """
    ทำความสะอาดเฉพาะคอลัมน์ object:
    - ลบ comma ในข้อความ (เพื่อไม่ให้ไปชนตอนแปลงเป็นตัวเลขในอนาคต)
    - แทนคำที่เป็น 'nan', 'None', 'null', '' เป็น None (ใน object เท่านั้น)
    หมายเหตุ: ยังไม่แปลงเป็น None ทั้ง df เพื่อไม่ให้ dtype แกว่ง
    """
    nan_strs = ['nan', 'NaN', 'None', 'null', '', 'NULL', 'NAN', 'Nan', 'none', 'NONE']
    obj_cols = df.select_dtypes(include='object').columns
    if len(obj_cols) == 0:
        return df
    # ลบ comma ก่อน
    df[obj_cols] = df[obj_cols].apply(lambda s: s.astype(str).str.replace(',', '', regex=False))
    # แทน string ว่าง/คำที่สื่อความหมายว่า null
    df[obj_cols] = df[obj_cols].replace(nan_strs, None)
    return df

def finalize_nulls_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """
    ขั้นตอนสุดท้ายก่อน upsert: แปลงค่าหายให้เป็น None เพื่อให้ DB ได้ NULL จริง
    (psycopg2/SQLAlchemy จะมอง None เป็น NULL)
    """
    return df.replace({pd.NA: None, np.nan: None})

# ✅ Load env
load_dotenv()

# ✅ Source (MariaDB)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)
task_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task"
)

# ✅ Target (PostgreSQL)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"keepalives": 1, "keepalives_idle": 30}
)

@op
def extract_commission_data():

    start_dt = '2025-01-01 00:00:00'
    end_dt = '2025-09-31 23:59:59'

    # start_dt, end_dt = _today_range_th()
    print(f"⏱️ Time window (TH): {start_dt} → {end_dt}")

    q_select_plan = text("""
        SELECT quo_num,id_cus,no_car,current_campaign 
        FROM fin_system_select_plan 
        WHERE datestart >= :start_dt AND datestart < :end_dt
          AND id_cus NOT LIKE '%%FIN-TestApp%%'
          AND id_cus NOT LIKE '%%FIN-TestApp3%%'
          AND id_cus NOT LIKE '%%FIN-TestApp2%%'
          AND id_cus NOT LIKE '%%FIN-TestApp-2025%%'
          AND id_cus NOT LIKE '%%FIN-Tester1%%'
          AND id_cus NOT LIKE '%%FIN-Tester2%%'
          AND id_cus NOT LIKE '%%FINTEST-01%%'
    """)

    q_fin_order = text("""SELECT quo_num,numpay,order_number FROM fin_order""")
    q_system_pay = text("""
        SELECT quo_num,show_com_prb,show_com_ins,discom,
               show_price_com_count,show_com_addon,condition_install,
               chanel_main, condi_com
        FROM fin_system_pay
    """)
    q_com_rank = text("""
        SELECT quo_num,
               SUM(com_invite) AS com_invite,
               SUM(com_rank)   AS com_rank,
               SUM(com_total)  AS total_commission
        FROM fin_com_rank
        GROUP BY quo_num
    """)
    q_fin_finance = text("""SELECT order_number, money_one ,money_ten FROM fin_finance""")
    q_wp_users = text("""
        SELECT cuscode as id_cus
        FROM wp_users 
        WHERE cuscode NOT IN ("FINTEST-01", "FIN-TestApp", "Admin-VIF", "adminmag_fin")
    """)
    q_per = text("""SELECT quo_num,de_per_install FROM fin_com_detail""")

    with source_engine.connect() as scon, task_engine.connect() as tcon:
        df_select_plan = pd.read_sql(q_select_plan, scon, params={"start_dt": start_dt, "end_dt": end_dt})
        df_fin_order   = pd.read_sql(q_fin_order, tcon)
        df_system_pay  = pd.read_sql(q_system_pay, scon)
        df_com_rank    = pd.read_sql(q_com_rank, scon)
        df_fin_finance = pd.read_sql(q_fin_finance, tcon)
        df_wp_users    = pd.read_sql(q_wp_users, scon)
        df_per         = pd.read_sql(q_per, scon)

    print("📦 df_select_plan:", df_select_plan.shape)
    print("📦 df_fin_order:", df_fin_order.shape)
    print("📦 df_system_pay:", df_system_pay.shape)
    print("📦 df_com_rank:", df_com_rank.shape)
    print("📦 df_fin_finance:", df_fin_finance.shape)
    print("📦 df_wp_users:", df_wp_users.shape)
    print("📦 df_per:", df_per.shape)

    return df_select_plan, df_fin_order, df_system_pay, df_com_rank, df_fin_finance, df_wp_users, df_per

@op
def clean_commission_data(data_tuple):
    df_select_plan, df_fin_order, df_system_pay, df_com_rank, df_fin_finance, df_wp_users, df_per = data_tuple

    # รวมตาราง
    df = df_select_plan.copy()
    df = df.merge(df_fin_order, on='quo_num', how='left')
    df = df.merge(df_system_pay, on='quo_num', how='left')
    df = df.merge(df_com_rank, on='quo_num', how='left')
    df = df.merge(df_fin_finance, on='order_number', how='left')
    df = df.merge(df_wp_users, on='id_cus', how='left')
    df = df.merge(df_per, on='quo_num', how='left')

    # log NaN หลัง merge
    nan_after_merge = df.isna().sum()
    if nan_after_merge.sum() > 0:
        print("\n⚠️ NaN values after merges:")
        for col, count in nan_after_merge[nan_after_merge > 0].items():
            print(f"  - {col}: {count} NaN values")

    # --- เช็ค comma เฉพาะคอลัมน์ที่คาดว่าจะเป็นตัวเลข (ก่อน clean) ---
    numeric_cols_to_check = [
        "numpay", "money_one", "money_ten", "discom", "show_com_ins",
        "total_commission", "show_com_prb", "show_price_com_count",
        "show_com_addon", "com_invite", "com_rank"
    ]
    print("\n🔍 Checking for comma values in numeric columns:")
    for col in numeric_cols_to_check:
        if col in df.columns:
            c = has_comma_value(df[col])
            if c > 0:
                # แสดงตัวอย่างที่เป็น string เท่านั้น
                examples = df.loc[df[col].map(lambda x: isinstance(x, str) and (',' in x)), col].head(3).tolist()
                print(f"  - {col}: {c} values with commas | examples: {examples}")

    # --- ทำความสะอาดฝั่ง object ก่อน (ไม่ไปแตะ numeric dtype) ---
    df = clean_object_nans(df)

    # --- แปลงคอลัมน์ตัวเลขเป็น nullable dtypes ---
    # รอบแรก: คอลัมน์ numeric ดั้งเดิม
    initial_num_cols_float = ["numpay", "money_one", "money_ten", "discom", "show_com_ins",
                              "total_commission", "show_com_prb", "show_price_com_count",
                              "show_com_addon", "com_invite", "com_rank"]
    for col in initial_num_cols_float:
        if col in df.columns:
            df[col] = to_nullable_float(df[col])

    # ฟังก์ชันตีความ condition_install -> สถานะ
    def calculate_condi_com_status(val):
        val = (val or "").strip() if isinstance(val, str) else ""
        if not val or len(val) < 4:
            return None
        if val.startswith("0000"):
            return "หารเท่าทุกงวด"
        percent_mapping = {"20": "งวดแรก 20%", "25": "งวดแรก 25%", "40": "งวดแรก 40%", "50": "งวดแรก 50%"}
        prefix2 = val[:2]
        if prefix2 in percent_mapping:
            return percent_mapping[prefix2]
        if len(val) >= 4 and val[:4].isdigit():
            first_payment = val[:4]
            if first_payment != "0000":
                try:
                    amount = int(first_payment)
                    return f"งวดแรกจ่าย {amount:,}"
                except ValueError:
                    pass
        return None

    df["conditions_install"] = df["condition_install"].map(calculate_condi_com_status)

    # condi_com: 4/5 = ไม่หักคอม, อื่น ๆ = หักคอม
    codes = pd.to_numeric(df['condi_com'], errors='coerce')
    df['condi_com'] = np.where(codes.isin([4, 5]), 'ไม่หักคอม', 'หักคอม')

    # commission: ถ้า numpay>1 ใช้ discom ไม่งั้นใช้ show_com_ins (รองรับ NA)
    def _calc_commission(row):
        numpay = row.get("numpay")
        if pd.notna(numpay) and numpay > 1:
            return row.get("discom")
        return row.get("show_com_ins")
    df["commission"] = df.apply(_calc_commission, axis=1).astype('Float64')

    # num_payout: ดึงท้าย 2 ตัวเลขจาก condition_install
    def _num_payout(val):
        s = str(val) if val is not None else ""
        tail2 = s[-2:] if len(s) >= 2 else ""
        if tail2.isnumeric():
            return int(tail2)
        return pd.NA
    df["num_payout"] = df["condition_install"].map(_num_payout).astype('Int64')

    # เปลี่ยนชื่อ/ทิ้งคอลัมน์
    rename_columns = {
        "quo_num": "quotation_num",
        "show_com_prb": "prb_commission",
        "show_com_ins": "ins_commission",
        "discom": "after_tax_commission",
        "show_price_com_count": "paid_commission",
        "show_com_addon": "commission_addon"
    }
    df = df.rename(columns=rename_columns)
    df = df.drop(columns=[
        'numpay', 'chanel_main', 'no_car', 'condition_install', 'money_one',
        'money_ten', 'order_number'
    ], errors='ignore')

    # ลบแถว quotation_num ว่าง แล้วคง unique โดยข้อมูลแน่นที่สุด
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)
    valid_mask = df['quo_num' if 'quo_num' in df.columns else 'quotation_num'].notna()  # เผื่อกรณี rename แล้ว
    if 'quotation_num' in df.columns:
        valid_mask = df['quotation_num'].astype(str).str.strip().ne('') & df['quotation_num'].notna()
        df = pd.concat([
            df[valid_mask].sort_values('non_empty_count', ascending=False).drop_duplicates('quotation_num'),
            df[~valid_mask]
        ])
    df = df.drop(columns=['non_empty_count'], errors='ignore')

    df.columns = df.columns.str.lower()

    # ทำความสะอาด NaN/inf เบื้องต้น (คง dtype เป็น nullable)
    # แก้ค่า +/-inf ให้เป็น NA
    num_cols_nullable = df.select_dtypes(include=['Float64', 'Int64']).columns
    for col in num_cols_nullable:
        col_vals = df[col]
        if pd.api.types.is_float_dtype(col_vals):
            df[col] = col_vals.mask(np.isinf(col_vals), pd.NA)

    # แปลงคอลัมน์ numeric สำคัญให้เป็น Float64 อีกรอบ (กันคอลัมน์ที่ยังเป็น object)
    numeric_columns = [
        'total_commission', 'ins_commission', 'prb_commission',
        'after_tax_commission', 'paid_commission', 'commission_addon',
        'commission', 'com_invite', 'com_rank'
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = to_nullable_float(df[col])

    df = df.rename(columns={'id_cus': 'agent_id'})

    # Logs
    nan_counts = df.isna().sum()
    if nan_counts.sum() > 0:
        print("\n⚠️ Found NA/NaN values after cleaning:")
        for col, count in nan_counts[nan_counts > 0].items():
            print(f"  - {col}: {count}")

    print("\n🔍 Final check for comma values after cleaning:")
    for col in numeric_columns:
        if col in df.columns:
            c = has_comma_value(df[col])
            print(f"  - {'⚠️' if c>0 else '✅'} {col}: {c} string values with commas")

    print("\n📊 Cleaning completed")
    return df

@op
def load_commission_data(df: pd.DataFrame):
    from sqlalchemy.exc import OperationalError
    import time

    table_name = 'fact_commission_temp'
    pk_column = 'quotation_num'
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds
    BATCH_SIZE = 1000

    # --- ตรวจสอบก่อนเขียน DB (ยังคง dtype แบบ nullable ใน pandas) ---
    final_nan_counts = df.isna().sum()
    if final_nan_counts.sum() > 0:
        print("\nℹ️ NA/NaN (nullable) before DB insertion (will be converted to NULL):")
        for col, count in final_nan_counts[final_nan_counts > 0].items():
            print(f"  - {col}: {count}")
    else:
        print("\n✅ No NA/NaN values before DB insertion")

    numeric_cols_db = ['total_commission', 'ins_commission', 'prb_commission',
                       'after_tax_commission', 'paid_commission', 'commission_addon',
                       'commission', 'com_invite', 'com_rank']
    print("\n🔍 Final comma check before database insertion:")
    for col in numeric_cols_db:
        if col in df.columns:
            c = has_comma_value(df[col])
            print(f"  - {'⚠️' if c>0 else '✅'} {col}: {c} string values with commas")

    # ดึงข้อมูลเดิม
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    df_existing = df_existing.drop_duplicates(subset=[pk_column])
    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    common_ids = set(df[pk_column]) & set(df_existing[pk_column])

    df_to_insert = df[df[pk_column].isin(new_ids)].copy()
    df_to_update = df[df[pk_column].isin(common_ids)].copy()

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_to_update)} rows")

    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # --- แปลงค่า missing เพื่อ DB: pd.NA/np.nan -> None ---
    df_to_insert = finalize_nulls_for_db(df_to_insert)
    df_to_update = finalize_nulls_for_db(df_to_update)

    # ✅ Insert
    if not df_to_insert.empty:
        with target_engine.begin() as conn:
            conn.execute(metadata.insert(), df_to_insert.to_dict(orient='records'))

    # ✅ Upsert แบบ batch + null-safe update
    if not df_to_update.empty:
        records = df_to_update.to_dict(orient='records')

        update_cols = [
            c.name for c in metadata.columns
            if c.name not in [pk_column, 'create_at', 'update_at']
        ]

        for attempt in range(MAX_RETRIES):
            try:
                with target_engine.begin() as conn:
                    for i in range(0, len(records), BATCH_SIZE):
                        batch = records[i:i + BATCH_SIZE]
                        ins = pg_insert(metadata).values(batch)
                        excluded = ins.excluded

                        set_map = {
                            c: func.coalesce(getattr(excluded, c), getattr(metadata.c, c))
                            for c in update_cols
                        }
                        set_map['update_at'] = func.now()

                        change_conditions = [
                            func.coalesce(getattr(excluded, c), getattr(metadata.c, c))\
                                .is_distinct_from(getattr(metadata.c, c))
                            for c in update_cols
                        ]

                        stmt = ins.on_conflict_do_update(
                            index_elements=[metadata.c[pk_column]],
                            set_=set_map,
                            where=or_(*change_conditions)
                        )
                        conn.execute(stmt)
                break
            except OperationalError as e:
                if "deadlock detected" in str(e) or "server closed the connection" in str(e):
                    print(f"⚠️ PostgreSQL error. Retrying in {RETRY_DELAY}s ({attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(RETRY_DELAY)
                else:
                    raise

    print("✅ Insert/update completed.")

@job
def fact_commission_etl():
    load_commission_data(clean_commission_data(extract_commission_data()))


if __name__ == "__main__":
    df_raw = extract_commission_data()

    df_clean = clean_commission_data((df_raw))
    print("✅ Cleaned columns:", df_clean.columns)

    # 🔍 Final check before loading to database
    final_nan_check = df_clean.isnull().sum()
    if final_nan_check.sum() > 0:
        print("\n⚠️ Final NaN check before database loading:")
        for col, count in final_nan_check[final_nan_check > 0].items():
            print(f"  - {col}: {count} NaN values")
    else:
        print("\n✅ No NaN values found before database loading")
    
    # 🔍 Final comma check before loading to database
    numeric_cols_main = ['total_commission', 'ins_commission', 'prb_commission',
                        'after_tax_commission', 'paid_commission', 'commission_addon',
                        'commission', 'com_invite', 'com_rank']
    
    print("\n🔍 Final comma check before database loading:")
    for col in numeric_cols_main:
        if col in df_clean.columns:
            comma_count = df_clean[col].astype(str).str.contains(',').sum()
            if comma_count > 0:
                print(f"  - ⚠️ {col}: {comma_count} values still have commas")
                # Show remaining examples
                examples = df_clean[df_clean[col].astype(str).str.contains(',', na=False)][col].head(3)
                print(f"    Remaining examples: {examples.tolist()}")
            else:
                print(f"  - ✅ {col}: No comma values found")
    
    # Show sample of cleaned numeric data
    print("\n📊 Sample of cleaned numeric data:")
    for col in numeric_cols_main[:3]:  # Show first 3 columns
        if col in df_clean.columns:
            sample_values = df_clean[col].dropna().head(3)
            print(f"  - {col}: {sample_values.tolist()}")

    # output_path = "fact_commission.csv"
    # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
    # print(f"💾 Saved to {output_path}")

    # output_path = "fact_commission.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_commission_data(df_clean)
    print("🎉 completed! Data upserted to fact_commission.")
