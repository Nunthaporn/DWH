from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

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
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_commission_data():
    df_select_plan = pd.read_sql("""
        SELECT quo_num,id_cus,no_car,current_campaign 
        FROM fin_system_select_plan 
        WHERE datestart >= '2025-01-01' AND datestart < '2025-07-01'
    """, source_engine)

    df_fin_order = pd.read_sql("""
        SELECT quo_num,numpay,order_number
        FROM fin_order 
        WHERE datekey >= '2025-01-01' AND datekey < '2025-07-01'
    """, task_engine)

    df_system_pay = pd.read_sql("""
        SELECT quo_num,show_com_total,show_com_prb,show_com_ins,discom,
               show_price_com_count,show_com_addon,condition_install,
               percent_install,chanel_main
        FROM fin_system_pay 
        WHERE datestart >= '2025-01-01' AND datestart < '2025-07-01'
    """, source_engine)

    df_com_rank = pd.read_sql("""
        SELECT quo_num,com_invite,com_rank
        FROM fin_com_rank 
        WHERE date_add >= '2025-01-01' AND date_add < '2025-07-01'
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

    df["numpay"] = pd.to_numeric(df["numpay"], errors="coerce")
    df["money_one"] = pd.to_numeric(df["money_one"], errors="coerce")
    df["money_ten"] = pd.to_numeric(df["money_ten"], errors="coerce")
    df["discom"] = pd.to_numeric(df["discom"], errors="coerce")
    df["show_com_ins"] = pd.to_numeric(df["show_com_ins"], errors="coerce")

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
    numeric_columns = [
        'total_commission', 'ins_commission', 'prb_commission',
        'after_tax_commission', 'paid_commission', 'commission_addon',
        'commission', 'com_invite', 'com_rank'
    ]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "", regex=False), errors="coerce")

    df = df.rename(columns={'id_cus': 'agent_id'})

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

    # 🧹 Clean NaN and inf
    nan_strs = ['nan', 'NaN', 'None', 'null', '']
    df = df.replace([np.inf, -np.inf], 0)
    df = df.replace(nan_strs, None)
    df = df.where(pd.notnull(df), None)

    df = df[~df[pk_column].duplicated(keep='first')].copy()

    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT {pk_column} FROM {table_name}", conn)

    df_existing = df_existing.drop_duplicates(subset=[pk_column])
    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    common_ids = set(df[pk_column]) & set(df_existing[pk_column])

    df_to_insert = df[df[pk_column].isin(new_ids)].copy()
    df_to_update = df[df[pk_column].isin(common_ids)].copy()

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_to_update)} rows")

    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    if not df_to_insert.empty:
        with target_engine.begin() as conn:
            conn.execute(metadata.insert(), df_to_insert.to_dict(orient='records'))

    if not df_to_update.empty:
        records = df_to_update.to_dict(orient='records')
        for attempt in range(MAX_RETRIES):
            try:
                with target_engine.begin() as conn:
                    stmt = pg_insert(metadata).values(records)

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
                if "deadlock detected" in str(e):
                    print(f"⚠️ Deadlock detected. Retrying in {RETRY_DELAY}s ({attempt + 1}/{MAX_RETRIES})...")
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

#     # output_path = "fact_commission.csv"
#     # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
#     # print(f"💾 Saved to {output_path}")

#     output_path = "fact_commission.xlsx"
#     df_clean.to_excel(output_path, index=False, engine='openpyxl')
#     print(f"💾 Saved to {output_path}")

    # load_commission_data(df_clean)
    # print("🎉 completed! Data upserted to fact_commission.")