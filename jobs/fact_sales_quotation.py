from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ✅ Load environment variables
load_dotenv()

# ✅ DB source (MariaDB)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)
source_engine_task = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task"
)

# ✅ DB target (PostgreSQL)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_sales_quotation_data():
    df_plan = pd.read_sql("""
        SELECT quo_num, type_insure, type_work, datestart, id_government_officer, status_gpf, quo_num_old,
               status AS status_fssp
        FROM fin_system_select_plan 
        WHERE datestart >= '2025-01-01' AND datestart < '2025-07-01'
          AND type_insure IN ('ประกันรถ', 'ตรอ')
    """, source_engine)

    df_order = pd.read_sql("""
        SELECT quo_num, order_number, chanel, datekey, status AS status_fo
        FROM fin_order
    """, source_engine_task)

    df_pay = pd.read_sql("""
        SELECT quo_num, datestart, numpay, show_price_ins, show_price_prb, show_price_total,
               show_price_check, show_price_service, show_price_taxcar, show_price_fine,
               show_price_addon, show_price_payment, distax, show_ems_price, show_discount_ins,
               discount_mkt, discount_government, discount_government_fin,
               discount_government_ins, coupon_addon, status AS status_fsp
        FROM fin_system_pay 
        WHERE datestart >= '2025-01-01' AND datestart < '2025-07-01'
          AND type_insure IN ('ประกันรถ', 'ตรอ')
    """, source_engine)

    return df_plan, df_order, df_pay


@op
def clean_sales_quotation_data(inputs):
    df, df1, df2 = inputs
    df_merged = pd.merge(df, df1, on='quo_num', how='left')
    df_merged = pd.merge(df_merged, df2, on='quo_num', how='left')

    # ✅ เปลี่ยนชื่อคอลัมน์
    df_merged.rename(columns={
        "quo_num": "quotation_num",
        "datestart_x": "quotation_date",
        "datestart_y": "transaction_date",
        "datekey": "order_time",
        "type_insure": "type_insurance",
        "type_work": "order_type",
        "id_government_officer": "rights_government",
        "status_gpf": "goverment_type",
        "quo_num_old": "quotation_num_old",
        "numpay": "installment_number",
        "show_price_ins": "ins_amount",
        "show_price_prb": "prb_amount",
        "show_price_total": "total_amount",
        "show_price_check": "show_price_check",
        "show_price_service": "service_price",
        "show_price_taxcar": "tax_car_price",
        "show_price_fine": "overdue_fine_price",
        "show_price_addon": "price_addon",
        "show_price_payment": "payment_amount",
        "distax": "tax_amount",
        "show_ems_price": "ems_amount",
        "show_discount_ins": "ins_discount",
        "discount_mkt": "mkt_discount",
        "discount_government": "goverment_discount",
        "discount_government_fin": "fin_goverment_discount",
        "discount_government_ins": "ins_goverment_discount",
        "coupon_addon": "discount_addon",
        "chanel": "contact_channel",
    }, inplace=True)

    df_merged.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df_merged.replace("NaN", np.nan, inplace=True)
    df_merged['transaction_date'] = pd.to_datetime(df_merged['transaction_date'], errors='coerce')
    df_merged['transaction_date'] = df_merged['transaction_date'].dt.strftime('%Y%m%d').astype('Int64')
    df_merged['order_time'] = pd.to_datetime(df_merged['order_time'], errors='coerce')
    df_merged['order_time'] = df_merged['order_time'].dt.strftime('%Y%m%d').astype('Int64')
    df_merged['quotation_date'] = pd.to_datetime(df_merged['quotation_date'], errors='coerce')
    df_merged['quotation_date'] = df_merged['quotation_date'].dt.strftime('%Y%m%d').astype('Int64')
    df_merged['installment_number'] = df_merged['installment_number'].replace({'0': '1', '03': '3', '06': '6', '08': '8'})

    # ✅ เพิ่มการสร้างคอลัมน์ `status`
    def map_status(row):
        if pd.notnull(row['status_fo']):
            if row['status_fo'] == '88':
                return 'cancel'
            return row['status_fo']
        s1 = row.get('status_fssp') or ''
        s2 = row.get('status_fsp') or ''
        key = (str(s1).strip(), str(s2).strip())
        mapping = {
            ('wait', ''): '1',
            ('wait-key', ''): '1',
            ('sendpay', 'sendpay'): '2',
            ('sendpay', 'verify-wait'): '2',
            ('tran-succ', 'sendpay'): '2',
            ('tran-succ', 'verify-wait'): '2',
            ('cancel', '88'): 'cancel',
            ('delete', ''): 'delete',
            ('wait', 'sendpay'): '2',
            ('delete', 'sendpay'): 'delete',
            ('delete', 'wait'): 'delete',
            ('delete', 'wait-key'): 'delete',
            ('wait', 'wait'): '1',
            ('wait', 'wait-key'): '1',
            ('', 'wait'): '1',
            ('cancel', ''): 'cancel',
            ('cancel', 'cancel'): 'cancel',
            ('delete', 'delete'): 'delete',
            ('active', 'verify'): '6',
            ('active', 'success'): '8',
            ('active', ''): '8'
        }
        return mapping.get(key, None)

    df_merged['status'] = df_merged.apply(map_status, axis=1)
    df_merged.drop(columns=['status_fssp', 'status_fsp', 'status_fo'], inplace=True)

    df_merged.drop_duplicates(subset=['quotation_num'], keep='first', inplace=True)
    df_merged = df_merged.applymap(lambda x: np.nan if isinstance(x, str) and x.strip().lower() == "nan" else x)
    df_merged = df_merged.where(pd.notnull(df_merged), None)

    return df_merged

@op
def load_sales_quotation_data(df: pd.DataFrame):
    table_name = 'fact_sales_quotation'
    pk_column = 'quotation_num'

    # ✅ กรอง fact_sales_quotation ซ้ำจาก DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()

    # ✅ Load ข้อมูลเดิมจาก PostgreSQL
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    # ✅ กรอง fact_sales_quotation ซ้ำจากข้อมูลเก่า
    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    # ✅ Identify fact_sales_quotation ใหม่ (ไม่มีใน DB)
    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    # ✅ Identify fact_sales_quotation ที่มีอยู่แล้ว
    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    # ✅ Merge ด้วย suffix (_new, _old)
    merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

    # ✅ ระบุคอลัมน์ที่ใช้เปรียบเทียบ (ยกเว้น key และ audit fields)
    exclude_columns = [pk_column, 'agent_id', 'customer_id','car_id','sales_id','order_type_id', 'payment_plan_id', 'create_at', 'update_at']
    compare_cols = [
        col for col in df.columns
        if col not in exclude_columns
        and f"{col}_new" in merged.columns
        and f"{col}_old" in merged.columns
    ]

    # ✅ ฟังก์ชันเปรียบเทียบอย่างปลอดภัยจาก pd.NA
    def is_different(row):
        for col in compare_cols:
            val_new = row.get(f"{col}_new")
            val_old = row.get(f"{col}_old")
            if pd.isna(val_new) and pd.isna(val_old):
                continue
            if val_new != val_old:
                return True
        return False

    # ✅ ตรวจหาความแตกต่างจริง
    df_diff = merged[merged.apply(is_different, axis=1)].copy()

    # ✅ เตรียม DataFrame สำหรับ update โดยใช้ fact_sales_quotation ปกติ (ไม่เติม _new)
    update_cols = [f"{col}_new" for col in compare_cols]
    all_cols = [pk_column] + update_cols

    df_diff_renamed = df_diff[all_cols].copy()
    df_diff_renamed.columns = [pk_column] + compare_cols  # เปลี่ยนชื่อ column ให้ตรงกับตารางจริง

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

    # ✅ Load table metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert (กรอง fact_sales_quotation ที่เป็น NaN)
    if not df_to_insert.empty:
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} insert rows with null fact_sales_quotation")
        if not df_to_insert_valid.empty:
            with target_engine.begin() as conn:
                conn.execute(metadata.insert(), df_to_insert_valid.to_dict(orient='records'))

    # ✅ Update
    if not df_diff_renamed.empty:
        with target_engine.begin() as conn:
            for record in df_diff_renamed.to_dict(orient='records'):
                stmt = pg_insert(metadata).values(**record)
                update_columns = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns
                    if c.name != pk_column
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")

@job
def fact_sales_quotation_etl():
    load_sales_quotation_data(clean_sales_quotation_data(extract_sales_quotation_data()))

# if __name__ == "__main__":
#     df_plan, df_order, df_pay = extract_sales_quotation_data()

#     print(f"- df_plan: {df_plan.shape}")
#     print(f"- df_order: {df_order.shape}")
#     print(f"- df_pay: {df_pay.shape}")

#     df_clean = clean_sales_quotation_data((df_plan, df_order, df_pay))
#     print("✅ Cleaned columns:", df_clean.columns)

#     output_path = "fact_sales_quotation.xlsx"
#     df_clean.to_excel(output_path, index=False, engine='openpyxl')
#     print(f"💾 Saved to {output_path}")

    # load_sales_quotation_data(df_clean)
    # print("🎉 completed! Data upserted to fact_sales_quotation.")