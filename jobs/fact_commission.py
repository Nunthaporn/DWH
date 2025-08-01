from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta

def clean_nan_values(df):
    """
    Comprehensive function to clean NaN values and remove commas from DataFrame
    """
    # Define all possible NaN representations
    nan_strs = ['nan', 'NaN', 'None', 'null', '', 'NULL', 'NAN', 'Nan', 'none', 'NONE']
    
    # Remove commas from all string columns first
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.replace(',', '', regex=False)
    
    # Replace all NaN strings with None
    df = df.replace(nan_strs, None)
    
    # Replace infinite values with None
    df = df.replace([np.inf, -np.inf], None)
    
    # Convert all remaining NaN to None
    df = df.where(pd.notnull(df), None)
    
    # Additional check for float columns
    for col in df.columns:
        if df[col].dtype in ['float64', 'float32']:
            df[col] = df[col].where(pd.notnull(df[col]), None)
    
    return df

def clean_numeric_column(series):
    """
    Clean numeric column by removing commas, spaces, and converting to numeric
    """
    # Convert to string and remove commas and spaces
    cleaned = series.astype(str).str.replace(',', '', regex=False)
    cleaned = cleaned.str.replace(' ', '', regex=False)
    
    # Convert to numeric, coercing errors to NaN
    cleaned = pd.to_numeric(cleaned, errors="coerce")
    
    # Convert NaN to None for database compatibility
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

target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"keepalives": 1, "keepalives_idle": 30}
)

@op
def extract_commission_data():
    now = datetime.now()

    start_time = now.replace(minute=0, second=0, microsecond=0)
    end_time = now.replace(minute=59, second=59, microsecond=999999)

    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S') 

    df_select_plan = pd.read_sql("""
        SELECT quo_num,id_cus,no_car,current_campaign 
        FROM fin_system_select_plan 
        WHERE datestart BETWEEN '{start_str}' AND '{end_str}'
    """, source_engine)

    df_fin_order = pd.read_sql("""
        SELECT quo_num,numpay,order_number
        FROM fin_order 
        WHERE datekey >= '2024-01-01' AND datekey < '2025-08-01'
    """, task_engine)

    df_system_pay = pd.read_sql("""
        SELECT quo_num,show_com_total,show_com_prb,show_com_ins,discom,
               show_price_com_count,show_com_addon,condition_install,
               percent_install,chanel_main
        FROM fin_system_pay 
        WHERE datestart BETWEEN '{start_str}' AND '{end_str}'
    """, source_engine)

    df_com_rank = pd.read_sql("""
        SELECT quo_num,com_invite,com_rank
        FROM fin_com_rank 
    """, source_engine)

    df_fin_finance = pd.read_sql("""
        SELECT order_number, money_one ,money_ten 
        FROM fin_finance
    """, task_engine)

    df_wp_users = pd.read_sql("""
        SELECT cuscode as id_cus,status_vip 
        FROM wp_users 
        WHERE cuscode NOT IN ("FINTEST-01", "FIN-TestApp", "Admin-VIF", "adminmag_fin")
    """, source_engine)

    print("📦 df_select_plan:", df_select_plan.shape)
    print("📦 df_fin_order:", df_fin_order.shape)
    print("📦 df_system_pay:", df_system_pay.shape)
    print("📦 df_com_rank:", df_com_rank.shape)
    print("📦 df_fin_finance:", df_fin_finance.shape)
    print("📦 df_wp_users:", df_wp_users.shape)

    return df_select_plan, df_fin_order, df_system_pay, df_com_rank, df_fin_finance, df_wp_users

@op
def clean_commission_data(data_tuple):
    df_select_plan, df_fin_order, df_system_pay, df_com_rank, df_fin_finance, df_wp_users = data_tuple

    df = df_select_plan.copy()
    df = df.merge(df_fin_order, on='quo_num', how='left')
    df = df.merge(df_system_pay, on='quo_num', how='left')
    df = df.merge(df_com_rank, on='quo_num', how='left')
    df = df.merge(df_fin_finance, on='order_number', how='left')
    df = df.merge(df_wp_users, on='id_cus', how='left')

    # 🔍 Check for NaN after merges
    nan_after_merge = df.isnull().sum()
    if nan_after_merge.sum() > 0:
        print("\n⚠️ NaN values after merges:")
        for col, count in nan_after_merge[nan_after_merge > 0].items():
            print(f"  - {col}: {count} NaN values")
    
    # 🔍 Check for comma values in numeric columns
    numeric_cols_to_check = ["numpay", "money_one", "money_ten", "discom", "show_com_ins", 
                            "show_com_total", "show_com_prb", "show_com_ins", "show_price_com_count", 
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


    df["condi_com_status"] = df.apply(calculate_condi_com_status, axis=1)

    def calculate_condi_com(row):
        try:
            if (
                row["percent_install"] == 0
                or (
                    row["current_campaign"] == "ผ่อนไม่หักคอม"
                    and pd.notna(row["id_cus"])
                    and not row["id_cus"].startswith("FTR")
                )
            ):
                return "ไม่หักคอม"
            if pd.notna(row["id_cus"]) and row["id_cus"].startswith("FTR"):
                return "ปกติ"
            if pd.notna(row["chanel_main"]) and "ผ่อน" not in row["chanel_main"]:
                return "ปกติ"
            if (
                row.get("status_vip") in ("yes", "yes-new")
                and row.get("numpay", 0) > 1
                and row.get("condition_install") == "000003"
                and str(row.get("percent_install")) in ["0.3", "0.5"]
            ):
                return "หักคอม 0.3,0.5"
            return "หักคอม"
        except Exception:
            return "หักคอม"

    df["condi_com"] = df.apply(calculate_condi_com, axis=1)
    df["commission"] = df.apply(
        lambda row: row["discom"] if row["numpay"] > 1 else row["show_com_ins"],
        axis=1
    )

    rename_columns = {
        "quo_num": "quotation_num",
        "show_com_total": "total_commission",
        "show_com_prb": "ins_commission",
        "show_com_ins": "prb_commission",
        "discom": "after_tax_commission",
        "show_price_com_count": "paid_commission",
        "show_com_addon": "commission_addon"
    }
    df = df.rename(columns=rename_columns)
    df = df.drop(columns=[
        'numpay', 'current_campaign', 'status_vip', 'percent_install',
        'chanel_main', 'no_car', 'condition_install', 'money_one',
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
    today_str = datetime.now().strftime('%Y-%m-%d')

    # ✅ Load เฉพาะข้อมูลวันนี้จาก PostgreSQL
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(
            f"SELECT * FROM {table_name} WHERE update_at >= '{today_str}'",
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

    # ✅ Update in batch
    if not df_to_update.empty:
        records = df_to_update.to_dict(orient='records')

        for attempt in range(MAX_RETRIES):
            try:
                with target_engine.begin() as conn:
                    for i in range(0, len(records), BATCH_SIZE):
                        batch = records[i:i + BATCH_SIZE]
                        stmt = pg_insert(metadata).values(batch)
                        update_dict = {
                            c.name: stmt.excluded[c.name]
                            for c in metadata.columns if c.name != pk_column
                        }
                        stmt = stmt.on_conflict_do_update(
                            index_elements=[pk_column],
                            set_=update_dict
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
    
    # 🔍 Final verification - check if any NaN values were inserted
    try:
        with target_engine.connect() as conn:
            # Check for any NULL values in numeric columns
            result = conn.execute(f"""
                SELECT 
                    column_name, 
                    data_type,
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN column_value IS NULL THEN 1 END) as null_count
                FROM (
                    SELECT 
                        c.column_name,
                        c.data_type,
                        CASE 
                            WHEN c.column_name = 'quotation_num' THEN t.quotation_num::text
                            WHEN c.column_name = 'agent_id' THEN t.agent_id::text
                            WHEN c.column_name = 'total_commission' THEN t.total_commission::text
                            WHEN c.column_name = 'ins_commission' THEN t.ins_commission::text
                            WHEN c.column_name = 'prb_commission' THEN t.prb_commission::text
                            WHEN c.column_name = 'after_tax_commission' THEN t.after_tax_commission::text
                            WHEN c.column_name = 'paid_commission' THEN t.paid_commission::text
                            WHEN c.column_name = 'commission_addon' THEN t.commission_addon::text
                            WHEN c.column_name = 'commission' THEN t.commission::text
                            WHEN c.column_name = 'com_invite' THEN t.com_invite::text
                            WHEN c.column_name = 'com_rank' THEN t.com_rank::text
                            WHEN c.column_name = 'condi_com_status' THEN t.condi_com_status::text
                            WHEN c.column_name = 'condi_com' THEN t.condi_com::text
                            ELSE NULL
                        END as column_value
                    FROM information_schema.columns c
                    CROSS JOIN {table_name} t
                    WHERE c.table_name = '{table_name}' 
                    AND c.table_schema = 'public'
                ) subq
                GROUP BY column_name, data_type
                ORDER BY column_name
            """)
            
            print("\n🔍 Database verification results:")
            for row in result:
                print(f"  - {row[0]} ({row[1]}): {row[3]} NULL out of {row[2]} total rows")
                
    except Exception as e:
        print(f"⚠️ Could not verify database: {e}")

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