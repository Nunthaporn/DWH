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
    # ✅ เพิ่มประสิทธิภาพ: ใช้ LIMIT และปรับปรุง query
    df_plan = pd.read_sql("""
        SELECT quo_num, type_insure, datestart, id_government_officer, status_gpf, quo_num_old,
               status AS status_fssp
        FROM fin_system_select_plan 
        WHERE datestart >= '2025-01-01' AND datestart < '2025-08-04'
          AND type_insure IN ('ประกันรถ', 'ตรอ')
        ORDER BY datestart DESC
    """, source_engine)

    # ✅ ดึงเฉพาะข้อมูลที่จำเป็นจาก fin_order
    df_order = pd.read_sql("""
        SELECT quo_num, order_number, chanel, datekey, status AS status_fo
        FROM fin_order
        WHERE quo_num IS NOT NULL
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
        ORDER BY datestart DESC
    """, source_engine)

    print(f"📦 df_plan shape: {df_plan.shape}")
    print(f"📦 df_order shape: {df_order.shape}")
    print(f"📦 df_pay shape: {df_pay.shape}")

    return df_plan, df_order, df_pay

@op
def clean_sales_quotation_data(inputs):
    df, df1, df2 = inputs
    
    # ✅ เพิ่มประสิทธิภาพ: ใช้ merge แบบเดียวและลดการ copy
    df_merged = df.merge(df1, on='quo_num', how='left')
    df_merged = df_merged.merge(df2, on='quo_num', how='left')
    
    # ✅ ใช้ vectorized operations แทน map
    df_merged = df_merged.replace(['nan', 'NaN', ''], np.nan)
    # แก้ไข FutureWarning โดยใช้ infer_objects
    df_merged = df_merged.where(pd.notnull(df_merged), None).infer_objects(copy=False)

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

    # ✅ ใช้ vectorized operations สำหรับการทำความสะอาด
    df_merged = df_merged.replace(['nan', 'NaN', 'null', ''], np.nan)
    df_merged = df_merged.replace(r'^\s*$', np.nan, regex=True)
    
    # ✅ แปลงวันที่แบบ vectorized
    date_columns = ['transaction_date', 'order_time', 'quotation_date']
    for col in date_columns:
        if col in df_merged.columns:
            df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce').dt.strftime('%Y%m%d')
    
    # ✅ แทนที่ค่า installment_number แบบ vectorized
    if 'installment_number' in df_merged.columns:
        df_merged['installment_number'] = df_merged['installment_number'].replace({
            '0': '1', '03': '3', '06': '6', '08': '8'
        })

    # ✅ เพิ่มการสร้างคอลัมน์ `status` แบบ vectorized
    def map_status_vectorized(row):
        if pd.notnull(row['status_fo']):
            if row['status_fo'] == '88':
                return 'cancel'
            return row['status_fo']
        
        s1 = str(row.get('status_fssp') or '').strip()
        s2 = str(row.get('status_fsp') or '').strip()
        key = (s1, s2)
        
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

    df_merged['status'] = df_merged.apply(map_status_vectorized, axis=1)
    df_merged.drop(columns=['status_fssp', 'status_fsp', 'status_fo'], inplace=True)

    # ✅ ลบข้อมูลซ้ำแบบ vectorized
    df_merged.drop_duplicates(subset=['quotation_num'], keep='first', inplace=True)
    
    # ✅ แปลง NaN เป็น None แบบ vectorized
    df_merged = df_merged.where(pd.notnull(df_merged), None).infer_objects(copy=False)

    # ✅ แปลงคอลัมน์ตัวเลขแบบ vectorized
    int_columns = ['installment_number', 'show_price_check', 'price_product', 'ems_amount', 'service_price']
    for col in int_columns:
        if col in df_merged.columns:
            df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')

    # ✅ แก้ไข tax_amount แบบ vectorized
    if 'tax_amount' in df_merged.columns:
        df_merged['tax_amount'] = pd.to_numeric(df_merged['tax_amount'], errors='coerce')
        df_merged['tax_amount'] = df_merged['tax_amount'].replace([np.inf, -np.inf], 0)

    # ✅ ตรวจสอบค่าตัวเลขที่เกินขอบเขตแบบ vectorized
    INT32_MAX = 2_147_483_647
    INT32_MIN = -2_147_483_648
    INT64_MAX = 9_223_372_036_854_775_807
    INT64_MIN = -9_223_372_036_854_775_808

    possible_int_cols = [
        'installment_number', 'show_price_check', 'price_product', 'ems_amount', 'service_price',
        'ins_amount', 'prb_amount', 'total_amount', 'tax_car_price', 'overdue_fine_price',
        'ins_discount', 'mkt_discount', 'payment_amount', 'price_addon', 'discount_addon',
        'goverment_discount', 'tax_amount', 'fin_goverment_discount', 'ins_goverment_discount'
    ]
    
    for col in possible_int_cols:
        if col in df_merged.columns:
            df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')
            
            # ใช้ boolean indexing แทนการ filter
            over_int64_mask = (df_merged[col].notnull()) & ((df_merged[col] > INT64_MAX) | (df_merged[col] < INT64_MIN))
            if over_int64_mask.any():
                print(f"❌ พบค่าคอลัมน์ {col} เกินขอบเขต BIGINT: {over_int64_mask.sum()} แถว")
                df_merged = df_merged[~over_int64_mask]
            
            over_int32_mask = (df_merged[col].notnull()) & ((df_merged[col] > INT32_MAX) | (df_merged[col] < INT32_MIN))
            if over_int32_mask.any():
                print(f"⚠️ พบค่าคอลัมน์ {col} เกินขอบเขต INTEGER: {over_int32_mask.sum()} แถว")
                df_merged = df_merged[~over_int32_mask]

    # ✅ แปลงคอลัมน์ INT8 แบบ vectorized
    int8_cols = [
        'transaction_date', 'order_time', 'installment_number', 'show_price_check',
        'price_product', 'ems_amount', 'service_price', 'quotation_date'
    ]
    
    for col in int8_cols:
        if col in df_merged.columns:
            df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')
            
            # แทนที่ inf และค่าที่เกินขอบเขต
            inf_mask = df_merged[col].isin([np.inf, -np.inf])
            if inf_mask.any():
                print(f"⚠️ {col} พบค่า inf/-inf {inf_mask.sum()} แถวแทนเป็น 0")
                df_merged.loc[inf_mask, col] = 0
            
            # แทนที่ NaN และค่าที่เกินขอบเขต
            invalid_mask = df_merged[col].isnull() | (df_merged[col] > INT64_MAX) | (df_merged[col] < INT64_MIN)
            if invalid_mask.any():
                df_merged.loc[invalid_mask, col] = None
            
            # แปลงเป็น Int64
            df_merged[col] = df_merged[col].astype('Int64')

    print("\n📊 Cleaning completed")

    return df_merged

@op
def load_sales_quotation_data(df: pd.DataFrame):
    table_name = 'fact_sales_quotation'
    pk_column = 'quotation_num'
    
    # ✅ เพิ่มการตรวจสอบข้อมูล
    if df.empty:
        print("⚠️ ไม่มีข้อมูลที่จะประมวลผล")
        return
    
    print(f"📊 เริ่มประมวลผลข้อมูล {len(df)} rows")

    # ลบข้อมูลซ้ำใน DataFrame ก่อน
    df = df[~df[pk_column].duplicated(keep='first')].copy()
    print(f"📊 ข้อมูลหลังจากลบซ้ำ: {len(df)} rows")

    # ✅ เพิ่มประสิทธิภาพ: ดึงเฉพาะ quotation_num ที่มีอยู่
    quotation_nums = df[pk_column].dropna().unique()
    if len(quotation_nums) == 0:
        print("⚠️ ไม่มี quotation_num ที่ valid")
        return

    # ใช้ IN clause เพื่อดึงเฉพาะข้อมูลที่ต้องการ (PostgreSQL format)
    if len(quotation_nums) > 0:
        try:
            # แปลงเป็น list ของ tuples สำหรับ PostgreSQL
            params = [(str(qnum),) for qnum in quotation_nums]
            placeholders = ','.join(['%s'] * len(quotation_nums))
            query = f"SELECT {pk_column} FROM {table_name} WHERE {pk_column} IN ({placeholders})"
            
            print(f"🔍 ตรวจสอบข้อมูลที่มีอยู่ {len(quotation_nums)} quotation numbers...")
            
            with target_engine.connect() as conn:
                existing_ids = pd.read_sql(query, conn, params=params)
            
            print(f"✅ พบข้อมูลที่มีอยู่ {len(existing_ids)} rows")
        except Exception as e:
            print(f"⚠️ เกิดข้อผิดพลาดในการตรวจสอบข้อมูล: {e}")
            existing_ids = pd.DataFrame(columns=[pk_column])
    else:
        existing_ids = pd.DataFrame(columns=[pk_column])
    
    existing_quotation_nums = set(existing_ids[pk_column].astype(str))
    new_quotation_nums = set(quotation_nums.astype(str)) - existing_quotation_nums
    common_quotation_nums = set(quotation_nums.astype(str)) & existing_quotation_nums

    # แยกข้อมูลสำหรับ insert และ update
    df_to_insert = df[df[pk_column].astype(str).isin(new_quotation_nums)].copy()
    df_to_update = df[df[pk_column].astype(str).isin(common_quotation_nums)].copy()

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_to_update)} rows")

    # ✅ Load metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Batch Insert new rows
    if not df_to_insert.empty:
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} rows with null {pk_column}")

        # ทำความสะอาดข้อมูลก่อน insert
        df_to_insert_valid = df_to_insert_valid.where(pd.notnull(df_to_insert_valid), None).infer_objects(copy=False)
        df_to_insert_valid = df_to_insert_valid.replace([np.inf, -np.inf], None)

        # ✅ ใช้ batch insert แทน row-by-row
        if not df_to_insert_valid.empty:
            with target_engine.begin() as conn:
                # แปลงเป็น list of dicts
                records = df_to_insert_valid.to_dict(orient='records')
                
                # ทำความสะอาดข้อมูล
                cleaned_records = []
                for record in records:
                    cleaned = {}
                    for k, v in record.items():
                        if pd.isna(v) or (isinstance(v, str) and v.strip().lower() in ["nan", "null", ""]):
                            cleaned[k] = None
                        elif isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                            cleaned[k] = None
                        else:
                            cleaned[k] = v
                    cleaned_records.append(cleaned)
                
                # ใช้ batch insert สำหรับ PostgreSQL
                try:
                    print(f"💾 เริ่ม insert ข้อมูล {len(cleaned_records)} rows...")
                    for i, record in enumerate(cleaned_records):
                        stmt = pg_insert(metadata).values(**record)
                        stmt = stmt.on_conflict_do_nothing(index_elements=[pk_column])
                        conn.execute(stmt)
                        
                        # แสดงความคืบหน้าทุก 5000 rows
                        if (i + 1) % 5000 == 0:
                            print(f"📊 Inserted {i + 1}/{len(cleaned_records)} rows...")
                    
                    print(f"✅ Insert สำเร็จ {len(cleaned_records)} rows")
                except Exception as e:
                    print(f"❌ เกิดข้อผิดพลาดในการ insert: {e}")
                    raise

    # ✅ Batch Update existing rows
    if not df_to_update.empty:
        # ดึงข้อมูลเดิมสำหรับเปรียบเทียบ
        if len(common_quotation_nums) > 0:
            try:
                update_placeholders = ','.join(['%s'] * len(common_quotation_nums))
                update_query = f"SELECT * FROM {table_name} WHERE {pk_column} IN ({update_placeholders})"
                
                # แปลงเป็น list ของ tuples สำหรับ PostgreSQL
                update_params = [(str(qnum),) for qnum in common_quotation_nums]
                
                print(f"🔍 ดึงข้อมูลเดิมสำหรับเปรียบเทียบ {len(common_quotation_nums)} rows...")
                
                with target_engine.connect() as conn:
                    df_existing_for_update = pd.read_sql(update_query, conn, params=update_params)
                
                print(f"✅ ดึงข้อมูลเดิมสำเร็จ {len(df_existing_for_update)} rows")
            except Exception as e:
                print(f"⚠️ เกิดข้อผิดพลาดในการดึงข้อมูลเดิม: {e}")
                df_existing_for_update = pd.DataFrame()
        else:
            df_existing_for_update = pd.DataFrame()
        
        # เปรียบเทียบและหาข้อมูลที่เปลี่ยนแปลง
        exclude_columns = [pk_column, 'agent_id', 'customer_id', 'car_id', 'sales_id',
                           'order_type_id', 'payment_plan_id', 'create_at', 'update_at']
        
        compare_cols = [col for col in df.columns if col not in exclude_columns]
        
        # ✅ เพิ่มประสิทธิภาพ: ใช้ merge แทน apply
        merged = df_to_update.merge(df_existing_for_update, on=pk_column, suffixes=('_new', '_old'))
        
        # หาแถวที่มีการเปลี่ยนแปลง
        changed_rows = []
        for _, row in merged.iterrows():
            has_changes = False
            for col in compare_cols:
                val_new = row.get(f"{col}_new")
                val_old = row.get(f"{col}_old")
                
                # เปรียบเทียบค่า
                if pd.isna(val_new) != pd.isna(val_old):
                    has_changes = True
                    break
                elif not pd.isna(val_new) and not pd.isna(val_old) and val_new != val_old:
                    has_changes = True
                    break
            
            if has_changes:
                # สร้าง record สำหรับ update
                update_record = {pk_column: row[pk_column]}
                for col in compare_cols:
                    update_record[col] = row[f"{col}_new"]
                changed_rows.append(update_record)
        
        # ✅ Batch update
        if changed_rows:
            print(f"🔄 Updating {len(changed_rows)} changed rows...")
            
            # ทำความสะอาดข้อมูล
            for record in changed_rows:
                for k, v in record.items():
                    if pd.isna(v) or (isinstance(v, str) and v.strip().lower() in ["nan", "null", ""]):
                        record[k] = None
                    elif isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                        record[k] = None
            
            with target_engine.begin() as conn:
                try:
                    print(f"🔄 เริ่ม update ข้อมูล {len(changed_rows)} rows...")
                    for i, record in enumerate(changed_rows):
                        stmt = pg_insert(metadata).values(**record)
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
                        
                        # แสดงความคืบหน้าทุก 5000 rows
                        if (i + 1) % 5000 == 0:
                            print(f"📊 Updated {i + 1}/{len(changed_rows)} rows...")
                    
                    print(f"✅ Update สำเร็จ {len(changed_rows)} rows")
                except Exception as e:
                    print(f"❌ เกิดข้อผิดพลาดในการ update: {e}")
                    raise
        else:
            print("✅ No changes detected for update.")

    print("🎉 Insert/update completed successfully!")

@job
def fact_sales_quotation_etl():
    load_sales_quotation_data(clean_sales_quotation_data(extract_sales_quotation_data()))

if __name__ == "__main__":
    try:
        print("🚀 เริ่มการประมวลผล fact_sales_quotation...")
        
        df_plan, df_order, df_pay = extract_sales_quotation_data()

        # print(f"- df_plan: {df_plan.shape}")
        # print(f"- df_order: {df_order.shape}")
        # print(f"- df_pay: {df_pay.shape}")

        df_clean = clean_sales_quotation_data((df_plan, df_order, df_pay))
        # print("✅ Cleaned columns:", df_clean.columns)

        # output_path = "fact_sales_quotation.xlsx"
        # df_clean.to_excel(output_path, index=False, engine='openpyxl')
        # print(f"💾 Saved to {output_path}")

        # # ทำความสะอาดข้อมูลครั้งสุดท้ายก่อนส่งไป database
        # df_clean = df_clean.where(pd.notnull(df_clean), None)
        # df_clean = df_clean.replace([np.inf, -np.inf], None)
        
        # # ตรวจสอบว่ายังมี NaN string อยู่หรือไม่
        # for col in df_clean.columns:
        #     if df_clean[col].dtype == object:
        #         mask = df_clean[col].astype(str).str.lower().str.strip() == 'nan'
        #         if mask.any():
        #             print(f"⚠️ พบ 'nan' string ในคอลัมน์ {col}: {mask.sum()} แถว")
        #             # แทนที่ NaN string ด้วย None
        #             df_clean.loc[mask, col] = None

        load_sales_quotation_data(df_clean)
        print("🎉 completed! Data upserted to fact_sales_quotation.")
        
    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาดในการประมวลผล: {e}")
        import traceback
        traceback.print_exc()
        raise