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

def clean_nan_values(df):
    """
    Comprehensive function to clean NaN values and remove commas from DataFrame
    """
    nan_strs = ['nan', 'NaN', 'None', 'null', '', 'NULL', 'NAN', 'Nan', 'none', 'NONE']

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.replace(',', '', regex=False)

    df = df.replace(nan_strs, None)
    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)

    for col in df.columns:
        if df[col].dtype in ['float64', 'float32']:
            df[col] = df[col].where(pd.notnull(df[col]), None)

    return df

def clean_numeric_column(series):
    cleaned = series.astype(str).str.replace(',', '', regex=False)
    cleaned = cleaned.str.replace(' ', '', regex=False)
    cleaned = pd.to_numeric(cleaned, errors="coerce")
    cleaned = cleaned.where(pd.notnull(cleaned), None)
    return cleaned

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
    # ⏱️ ดึงเฉพาะข้อมูล "วันนี้" ตามเวลาไทย
    start_dt, end_dt = _today_range_th()
    print(f"⏱️ Time window (TH): {start_dt} → {end_dt}")

    # ใช้พารามิเตอร์ ปลอดภัยกว่า และง่ายต่อการเปลี่ยนช่วงเวลาในอนาคต
    q_select_plan = text("""
        SELECT quo_num,id_cus,no_car,current_campaign 
        FROM fin_system_select_plan 
        WHERE update_at >= :start_dt AND update_at < :end_dt
          AND id_cus NOT LIKE '%%FIN-TestApp%%'
          AND id_cus NOT LIKE '%%FIN-TestApp3%%'
          AND id_cus NOT LIKE '%%FIN-TestApp2%%'
          AND id_cus NOT LIKE '%%FIN-TestApp-2025%%'
          AND id_cus NOT LIKE '%%FIN-Tester1%%'
          AND id_cus NOT LIKE '%%FIN-Tester2%%'
          AND id_cus NOT LIKE '%%FINTEST-01%%'
    """)

    # ตารางอื่นอาจไม่ต้องกรองเวลา (ข้อมูลอ้างอิง/สะสม) แต่ถ้าต้องการก็เพิ่ม WHERE แบบเดียวกันได้
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

    df = df_select_plan.copy()
    df = df.merge(df_fin_order, on='quo_num', how='left')
    df = df.merge(df_system_pay, on='quo_num', how='left')
    df = df.merge(df_com_rank, on='quo_num', how='left')
    df = df.merge(df_fin_finance, on='order_number', how='left')
    df = df.merge(df_wp_users, on='id_cus', how='left')
    df = df.merge(df_per, on='quo_num', how='left')

    # 🔍 Check for NaN after merges
    nan_after_merge = df.isnull().sum()
    if nan_after_merge.sum() > 0:
        print("\n⚠️ NaN values after merges:")
        for col, count in nan_after_merge[nan_after_merge > 0].items():
            print(f"  - {col}: {count} NaN values")
    
    # 🔍 Check for comma values in numeric columns
    numeric_cols_to_check = ["numpay", "money_one", "money_ten", "discom", "show_com_ins", 
                            "total_commission", "show_com_prb", "show_com_ins", "show_price_com_count", 
                            "show_com_addon", "com_invite", "com_rank"]
    
    print("\n🔍 Checking for comma values in numeric columns:")
    for col in numeric_cols_to_check:
        if col in df.columns:
            comma_count = df[col].astype(str).str.contains(',').sum()
            if comma_count > 0:
                print(f"  - {col}: {comma_count} values with commas")
                # Show some examples before cleaning
                examples_before = df[df[col].astype(str).str.contains(',', na=False)][col].head(3)
                print(f"    Examples before cleaning: {examples_before.tolist()}")
                
                # Show examples after cleaning
                cleaned_examples = clean_numeric_column(examples_before)
                print(f"    Examples after cleaning: {cleaned_examples.tolist()}")

    # Clean and convert numeric columns with better NaN handling
    numeric_cols_initial = ["numpay", "money_one", "money_ten", "discom", "show_com_ins"]
    
    for col in numeric_cols_initial:
        if col in df.columns:
            # Clean numeric column using helper function
            df[col] = clean_numeric_column(df[col])

    def calculate_condi_com_status(row):
        val = str(row.get("condition_install", "")).strip()

        if not val or len(val) < 4:
            return None

        # 💡 กรณีหารเท่าทุกงวด
        if val.startswith("0000"):
            return "หารเท่าทุกงวด"

        # 💡 กรณีงวดแรกจ่ายเป็น %
        percent_mapping = {
            "20": "งวดแรก 20%",
            "25": "งวดแรก 25%",
            "40": "งวดแรก 40%",
            "50": "งวดแรก 50%",
        }

        prefix2 = val[:2]
        if prefix2 in percent_mapping:
            return percent_mapping[prefix2]

        # 💡 กรณีงวดแรกจ่ายเป็นจำนวนเงิน เช่น 4000 5000
        if len(val) >= 4 and val[:4].isdigit():
            first_payment = val[:4]
            if first_payment != "0000":
                try:
                    amount = int(first_payment)
                    return f"งวดแรกจ่าย {amount:,}"
                except ValueError:
                    pass

        return None


    df["conditions_install"] = df.apply(calculate_condi_com_status, axis=1)

    codes = pd.to_numeric(df['condi_com'], errors='coerce')

    # แม็ปตามกติกา: 4 หรือ 5 => "ไม่หักคอม", อื่น ๆ (รวม None/NaN) => "หักคอม"
    df['condi_com'] = np.where(codes.isin([4, 5]), 'ไม่หักคอม', 'หักคอม')

    df["commission"] = df.apply(
        lambda row: row["discom"] if row["numpay"] > 1 else row["show_com_ins"],
        axis=1
    )

    df["num_payout"] = (
        df["condition_install"]
        .astype(str)
        .str[-2:]
        .where(lambda x: x.str.isnumeric())
        .astype(float)
        .astype("Int64")
    )

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

    df = df.replace(r'^\s*$', np.nan, regex=True)
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)
    valid_mask = df['quotation_num'].astype(str).str.strip().ne('') & df['quotation_num'].notna()
    df = pd.concat([
        df[valid_mask].sort_values('non_empty_count', ascending=False).drop_duplicates('quotation_num'),
        df[~valid_mask]
    ])
    df = df.drop(columns=['non_empty_count'])

    df.columns = df.columns.str.lower()
    
    # 🧹 Clean NaN values before numeric conversion
    df = clean_nan_values(df)
    
    numeric_columns = [
        'total_commission', 'ins_commission', 'prb_commission',
        'after_tax_commission', 'paid_commission', 'commission_addon',
        'commission', 'com_invite', 'com_rank'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            # Clean numeric column using helper function
            df[col] = clean_numeric_column(df[col])

    df = df.rename(columns={'id_cus': 'agent_id'})

    # 🔍 Debug: Check for NaN values after cleaning
    nan_counts = df.isnull().sum()
    if nan_counts.sum() > 0:
        print("\n⚠️ Found NaN values after cleaning:")
        for col, count in nan_counts[nan_counts > 0].items():
            print(f"  - {col}: {count} NaN values")
    
    # Check for infinite values
    inf_counts = np.isinf(df.select_dtypes(include=[np.number])).sum()
    if inf_counts.sum() > 0:
        print("\n⚠️ Found infinite values:")
        for col, count in inf_counts[inf_counts > 0].items():
            print(f"  - {col}: {count} infinite values")
    
    # 🔍 Check for remaining comma values after cleaning
    numeric_cols_final = ['total_commission', 'ins_commission', 'prb_commission',
                         'after_tax_commission', 'paid_commission', 'commission_addon',
                         'commission', 'com_invite', 'com_rank']
    
    print("\n🔍 Final check for comma values after cleaning:")
    for col in numeric_cols_final:
        if col in df.columns:
            comma_count = df[col].astype(str).str.contains(',').sum()
            if comma_count > 0:
                print(f"  - ⚠️ {col}: {comma_count} values still have commas")
            else:
                print(f"  - ✅ {col}: No comma values found")

    print("\n📊 Cleaning completed")

    return df

@op
def load_commission_data(df: pd.DataFrame):
    import numpy as np
    from sqlalchemy.exc import OperationalError
    import time

    table_name = 'fact_commission'
    pk_column = 'quotation_num'
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds
    BATCH_SIZE = 1000  # 👈 batch size สำหรับ update

    # 🧹 Clean NaN and inf - Using comprehensive cleaning function
    df = clean_nan_values(df)

    # 🔍 Final check before database insertion
    final_nan_counts = df.isnull().sum()
    if final_nan_counts.sum() > 0:
        print("\n⚠️ Final NaN check before database insertion:")
        for col, count in final_nan_counts[final_nan_counts > 0].items():
            print(f"  - {col}: {count} NaN values")
    else:
        print("\n✅ No NaN values found before database insertion")
    
    # 🔍 Final check for comma values before database insertion
    numeric_cols_db = ['total_commission', 'ins_commission', 'prb_commission',
                      'after_tax_commission', 'paid_commission', 'commission_addon',
                      'commission', 'com_invite', 'com_rank']
    
    print("\n🔍 Final comma check before database insertion:")
    for col in numeric_cols_db:
        if col in df.columns:
            comma_count = df[col].astype(str).str.contains(',').sum()
            if comma_count > 0:
                print(f"  - ⚠️ {col}: {comma_count} values still have commas")
                # Show examples
                examples = df[df[col].astype(str).str.contains(',', na=False)][col].head(3)
                print(f"    Examples: {examples.tolist()}")
            else:
                print(f"  - ✅ {col}: No comma values found")

    # ✅ วันปัจจุบัน (เริ่มต้นเวลา 00:00:00)
    # today_str = datetime.now().strftime('%Y-%m-%d')

    # ✅ Load เฉพาะข้อมูลวันนี้จาก PostgreSQL
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(
            f"SELECT * FROM {table_name}",
            conn
        )

    df_existing = df_existing.drop_duplicates(subset=[pk_column])
    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    common_ids = set(df[pk_column]) & set(df_existing[pk_column])

    df_to_insert = df[df[pk_column].isin(new_ids)].copy()
    df_to_update = df[df[pk_column].isin(common_ids)].copy()

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_to_update)} rows")

    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert
    if not df_to_insert.empty:
        with target_engine.begin() as conn:
            conn.execute(metadata.insert(), df_to_insert.to_dict(orient='records'))

    if not df_to_update.empty:
        records = df_to_update.to_dict(orient='records')

        # ระบุคอลัมน์ที่ให้อัปเดต (ไม่นับ PK/audit)
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
                        excluded = ins.excluded  # EXCLUDED.<col> ฝั่งค่ามาใหม่

                        # ไม่เขียนทับด้วย NULL: ใช้ COALESCE(EXCLUDED.col, table.col)
                        set_map = {
                            c: func.coalesce(getattr(excluded, c), getattr(metadata.c, c))
                            for c in update_cols
                        }
                        # update_at = now() เฉพาะเมื่อมีการ UPDATE จริง
                        set_map['update_at'] = func.now()

                        # WHERE เฉพาะเมื่อ "ค่าหลัง COALESCE" แตกต่างจากค่าปัจจุบัน (null-safe)
                        change_conditions = [
                            func.coalesce(getattr(excluded, c), getattr(metadata.c, c))\
                                .is_distinct_from(getattr(metadata.c, c))
                            for c in update_cols
                        ]

                        stmt = ins.on_conflict_do_update(
                            index_elements=[metadata.c[pk_column]],  # ต้องมี unique/PK บน quotation_num
                            set_=set_map,
                            where=or_(*change_conditions)
                        )
                        conn.execute(stmt)
                break  # ✅ success
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

# if __name__ == "__main__":
#     df_raw = extract_commission_data()

#     df_clean = clean_commission_data((df_raw))
#     print("✅ Cleaned columns:", df_clean.columns)

#     # 🔍 Final check before loading to database
#     final_nan_check = df_clean.isnull().sum()
#     if final_nan_check.sum() > 0:
#         print("\n⚠️ Final NaN check before database loading:")
#         for col, count in final_nan_check[final_nan_check > 0].items():
#             print(f"  - {col}: {count} NaN values")
#     else:
#         print("\n✅ No NaN values found before database loading")
    
#     # 🔍 Final comma check before loading to database
#     numeric_cols_main = ['total_commission', 'ins_commission', 'prb_commission',
#                         'after_tax_commission', 'paid_commission', 'commission_addon',
#                         'commission', 'com_invite', 'com_rank']
    
#     print("\n🔍 Final comma check before database loading:")
#     for col in numeric_cols_main:
#         if col in df_clean.columns:
#             comma_count = df_clean[col].astype(str).str.contains(',').sum()
#             if comma_count > 0:
#                 print(f"  - ⚠️ {col}: {comma_count} values still have commas")
#                 # Show remaining examples
#                 examples = df_clean[df_clean[col].astype(str).str.contains(',', na=False)][col].head(3)
#                 print(f"    Remaining examples: {examples.tolist()}")
#             else:
#                 print(f"  - ✅ {col}: No comma values found")
    
#     # Show sample of cleaned numeric data
#     print("\n📊 Sample of cleaned numeric data:")
#     for col in numeric_cols_main[:3]:  # Show first 3 columns
#         if col in df_clean.columns:
#             sample_values = df_clean[col].dropna().head(3)
#             print(f"  - {col}: {sample_values.tolist()}")

#     # output_path = "fact_commission.csv"
#     # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
#     # print(f"💾 Saved to {output_path}")

#     # output_path = "fact_commission.xlsx"
#     # df_clean.to_excel(output_path, index=False, engine='openpyxl')
#     # print(f"💾 Saved to {output_path}")

#     load_commission_data(df_clean)
#     print("🎉 completed! Data upserted to fact_commission.")
