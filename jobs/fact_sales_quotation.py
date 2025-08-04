from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
import datetime

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
        SELECT quo_num, type_insure, datestart, id_government_officer, status_gpf, quo_num_old,
               status AS status_fssp
        FROM fin_system_select_plan 
        WHERE datestart >= '2025-01-01' AND datestart < '2025-08-04'
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
        WHERE datestart >= '2025-01-01' AND datestart < '2025-08-04'
          AND type_insure IN ('ประกันรถ', 'ตรอ')
    """, source_engine)

    print(f"📦 df_plan shape: {df_plan.shape}")
    print(f"📦 df_order shape: {df_order.shape}")
    print(f"📦 df_pay shape: {df_pay.shape}")

    return df_plan, df_order, df_pay


@op
def clean_sales_quotation_data(inputs):
    df, df1, df2 = inputs
    df_merged = pd.merge(df, df1, on='quo_num', how='left')
    df_merged = pd.merge(df_merged, df2, on='quo_num', how='left')
    df_merged = df_merged.map(lambda x: np.nan if isinstance(x, str) and x.strip().lower() == "nan" else x)
    df_merged = df_merged.where(pd.notnull(df_merged), None)

    # ✅ เปลี่ยนชื่อคอลัมน์
    df_merged.rename(columns={
        "quo_num": "quotation_num",
        "datestart_x": "quotation_date",
        "datestart_y": "transaction_date",
        "datekey": "order_time",
        "type_insure": "type_insurance",
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

    df_merged = df_merged.map(
        lambda x: np.nan if isinstance(x, str) and x.strip().lower() in {"nan", "null", ""} else x
    )
    df_merged.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df_merged.replace("NaN", np.nan, inplace=True)
    df_merged['transaction_date'] = pd.to_datetime(df_merged['transaction_date'], errors='coerce')
    df_merged['transaction_date'] = df_merged['transaction_date'].dt.strftime('%Y%m%d')
    df_merged['order_time'] = pd.to_datetime(df_merged['order_time'], errors='coerce')
    df_merged['order_time'] = df_merged['order_time'].dt.strftime('%Y%m%d')
    df_merged['quotation_date'] = pd.to_datetime(df_merged['quotation_date'], errors='coerce')
    df_merged['quotation_date'] = df_merged['quotation_date'].dt.strftime('%Y%m%d')
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
    df_merged = df_merged.map(lambda x: np.nan if isinstance(x, str) and x.strip().lower() == "nan" else x)
    df_merged = df_merged.where(pd.notnull(df_merged), None)

    def clean_numeric_columns(df: pd.DataFrame, numeric_cols: list[str]):
        for col in numeric_cols:
            if col in df.columns:
                def safe_clean(val):
                    if pd.isna(val):  # ปล่อย NaN, None ผ่านเลย
                        return np.nan
                    if isinstance(val, str):
                        val_clean = val.replace(",", "").strip().lower()
                        if val_clean in {"nan", "null", ""}:
                            return np.nan
                        try:
                            return float(val_clean)
                        except ValueError:
                            return np.nan
                    return val  # ถ้าเป็นตัวเลขอยู่แล้ว เช่น int, float
                df[col] = df[col].apply(safe_clean)
        return df

    # ✅ แปลง NaN ให้เป็น None สำหรับ PostgreSQL
    df_merged = df_merged.where(pd.notnull(df_merged), None)

    int_columns = ['installment_number', 'show_price_check', 'price_product', 'ems_amount', 'service_price']

    for col in int_columns:
        if col in df_merged.columns:
            df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')   
    # แปลง NaN ทั่วไปให้เป็น None สำหรับ PostgreSQL
    df_merged = df_merged.where(pd.notnull(df_merged), None)

    # ✅ แก้ไข tax_amount ที่เป็น Infinity หรือ -Infinity ให้เป็น 0
    if 'tax_amount' in df_merged.columns:
        df_merged['tax_amount'] = pd.to_numeric(df_merged['tax_amount'], errors='coerce')
        df_merged['tax_amount'] = df_merged['tax_amount'].replace([np.inf, -np.inf], 0)

    # ✅ ตรวจสอบค่าตัวเลขที่เกินขอบเขต integer/bigint
    INT32_MAX = 2_147_483_647
    INT32_MIN = -2_147_483_648
    INT64_MAX = 9_223_372_036_854_775_807
    INT64_MIN = -9_223_372_036_854_775_808

    # สมมติว่าคอลัมน์ที่อาจเป็น integer/bigint
    possible_int_cols = [
        'installment_number', 'show_price_check', 'price_product', 'ems_amount', 'service_price',
        'ins_amount', 'prb_amount', 'total_amount', 'tax_car_price', 'overdue_fine_price',
        'ins_discount', 'mkt_discount', 'payment_amount', 'price_addon', 'discount_addon',
        'goverment_discount', 'tax_amount', 'fin_goverment_discount', 'ins_goverment_discount'
    ]
    for col in possible_int_cols:
        if col in df_merged.columns:
            # แปลงคอลัมน์เป็น numeric ก่อนเปรียบเทียบ
            df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')
            over_int64 = df_merged[(df_merged[col].notnull()) & ((df_merged[col] > INT64_MAX) | (df_merged[col] < INT64_MIN))]
            if not over_int64.empty:
                print(f"❌ พบค่าคอลัมน์ {col} เกินขอบเขต BIGINT:")
                print(over_int64[[col, 'quotation_num']])
                df_merged = df_merged.drop(over_int64.index)
            over_int32 = df_merged[(df_merged[col].notnull()) & ((df_merged[col] > INT32_MAX) | (df_merged[col] < INT32_MIN))]
            if not over_int32.empty:
                print(f"⚠️ พบค่าคอลัมน์ {col} เกินขอบเขต INTEGER:")
                print(over_int32[[col, 'quotation_num']])
                df_merged = df_merged.drop(over_int32.index)

    # --- INT8 columns ตาม schema ---
    int8_cols = [
        'transaction_date', 'order_time', 'installment_number', 'show_price_check',
        'price_product', 'ems_amount', 'service_price', 'quotation_date'
    ]
    for col in int8_cols:
        if col in df_merged.columns:
            df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')
            mask_inf = df_merged[col].apply(lambda x: x == np.inf or x == -np.inf)
            if mask_inf.any():
                print(f"⚠️ {col} พบค่า inf/-inf {mask_inf.sum()} แถวแทนเป็น 0")
                df_merged.loc[mask_inf, col] = 0
            mask_invalid = df_merged[col].isnull()
            if mask_invalid.any():
                df_merged.loc[mask_invalid, col] = None
            mask_over = (df_merged[col].notnull()) & ((df_merged[col] > INT64_MAX) | (df_merged[col] < INT64_MIN))
            if mask_over.any():
                print(f"❌ {col} พบค่ามาก/น้อยเกินขอบเขต int8 {mask_over.sum()} แถวแทนเป็น None (NULL)")
                df_merged.loc[mask_over, col] = None
            # แปลงเป็น pd.Int64Dtype() เพื่อให้ NULL/None รองรับและเป็น integer จริง
            df_merged[col] = df_merged[col].astype('Int64')

    print("\n📊 Cleaning completed")

    return df_merged

@op
def load_sales_quotation_data(df: pd.DataFrame):
    table_name = 'fact_sales_quotation'
    pk_column = 'quotation_num'

    # ลบข้อมูลซ้ำใน DataFrame ก่อน
    df = df[~df[pk_column].duplicated(keep='first')].copy()
    print(f"📊 ข้อมูลหลังจากลบซ้ำ: {len(df)} rows")

    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔍 Comparing {len(common_ids)} common rows for update...")

    df_diff_renamed = pd.DataFrame()

    if not df_common_new.empty and not df_common_old.empty:
        merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

        exclude_columns = [pk_column, 'agent_id', 'customer_id', 'car_id', 'sales_id',
                           'order_type_id', 'payment_plan_id', 'create_at', 'update_at']

        compare_cols = [
            col for col in df.columns
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
                if pd.isna(val_new) != pd.isna(val_old):
                    return True
                if not pd.isna(val_new) and not pd.isna(val_old) and val_new != val_old:
                    return True
            return False

        df_diff = merged[merged.apply(is_different, axis=1)].copy()

        if not df_diff.empty:
            update_cols = [f"{col}_new" for col in compare_cols]
            all_cols = [pk_column] + update_cols
            df_diff_renamed = df_diff[all_cols].copy()
            df_diff_renamed.columns = [pk_column] + compare_cols
            print(f"🔄 Update: {len(df_diff_renamed)} rows")
        else:
            print("✅ No differences found for update.")
    else:
        print("⚠️ No common quotations found between new and existing data.")

    # ✅ Load metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert new rows
    if not df_to_insert.empty:
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} rows with null {pk_column}")

        # ทำความสะอาดข้อมูลก่อน insert
        df_to_insert_valid = df_to_insert_valid.where(pd.notnull(df_to_insert_valid), None)
        df_to_insert_valid = df_to_insert_valid.replace([np.inf, -np.inf], None)

        def clean_record_for_db(record):
            cleaned = {}
            for k, v in record.items():
                if pd.isna(v) or (isinstance(v, str) and v.strip().lower() in ["nan", "null", ""]):
                    cleaned[k] = None
                elif isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                    cleaned[k] = None
                else:
                    cleaned[k] = v
            return cleaned

        if not df_to_insert_valid.empty:
            with target_engine.begin() as conn:
                for record in [clean_record_for_db(r) for r in df_to_insert_valid.to_dict(orient='records')]:
                    stmt = pg_insert(metadata).values(**record)
                    stmt = stmt.on_conflict_do_nothing(index_elements=[pk_column])
                    conn.execute(stmt)

    # ✅ Update existing rows
    if not df_diff_renamed.empty:
        # ทำความสะอาดข้อมูลก่อน update
        df_diff_renamed = df_diff_renamed.where(pd.notnull(df_diff_renamed), None)
        df_diff_renamed = df_diff_renamed.replace([np.inf, -np.inf], None)
        
        with target_engine.begin() as conn:
            for record in df_diff_renamed.to_dict(orient='records'):
                # ทำความสะอาด record ก่อนส่งไป database
                cleaned_record = {}
                for k, v in record.items():
                    if pd.isna(v) or (isinstance(v, str) and v.strip().lower() in ["nan", "null", ""]):
                        cleaned_record[k] = None
                    elif isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                        cleaned_record[k] = None
                    else:
                        cleaned_record[k] = v
                
                stmt = pg_insert(metadata).values(**cleaned_record)
                update_dict = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns if c.name not in [pk_column, 'create_at', 'update_at']
                }
                update_dict['update_at'] = datetime.datetime.now()
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_dict
                )
                conn.execute(stmt)

    print("🎉 Insert/update completed.")

@job
def fact_sales_quotation_etl():
    load_sales_quotation_data(clean_sales_quotation_data(extract_sales_quotation_data()))

# if __name__ == "__main__":
#     df_plan, df_order, df_pay = extract_sales_quotation_data()

#     # print(f"- df_plan: {df_plan.shape}")
#     # print(f"- df_order: {df_order.shape}")
#     # print(f"- df_pay: {df_pay.shape}")

#     df_clean = clean_sales_quotation_data((df_plan, df_order, df_pay))
#     # print("✅ Cleaned columns:", df_clean.columns)

#     # output_path = "fact_sales_quotation.xlsx"
#     # df_clean.to_excel(output_path, index=False, engine='openpyxl')
#     # print(f"💾 Saved to {output_path}")

#     # ทำความสะอาดข้อมูลครั้งสุดท้ายก่อนส่งไป database
#     df_clean = df_clean.where(pd.notnull(df_clean), None)
#     df_clean = df_clean.replace([np.inf, -np.inf], None)
    
#     # ตรวจสอบว่ายังมี NaN string อยู่หรือไม่
#     for col in df_clean.columns:
#         if df_clean[col].dtype == object:
#             mask = df_clean[col].astype(str).str.lower().str.strip() == 'nan'
#             if mask.any():
#                 print(f"⚠️ พบ 'nan' string ในคอลัมน์ {col}: {mask.sum()} แถว")
#                 # แทนที่ NaN string ด้วย None
#                 df_clean.loc[mask, col] = None

#     load_sales_quotation_data(df_clean)
#     print("🎉 completed! Data upserted to fact_sales_quotation.")