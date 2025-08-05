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

# ✅ DB source (MariaDB) - เพิ่ม timeout และ connection pool
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300
    }
)
source_engine_task = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300
    }
)

# ✅ DB target (PostgreSQL) - เพิ่ม timeout และ connection pool
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={
        'connect_timeout': 60,
        'options': '-c statement_timeout=300000'  # 5 minutes timeout
    }
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
        LIMIT 100000
    """, source_engine)

    # ✅ ดึงเฉพาะข้อมูลที่จำเป็นจาก fin_order และเพิ่ม LIMIT
    df_order = pd.read_sql("""
        SELECT quo_num, order_number, chanel, datekey, status AS status_fo
        FROM fin_order
        WHERE quo_num IS NOT NULL
        LIMIT 100000
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
        LIMIT 100000
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

    # ✅ ใช้ batch processing สำหรับการตรวจสอบข้อมูลที่มีอยู่
    if len(quotation_nums) > 0:
        try:
            # แบ่งเป็น batches เล็กๆ เพื่อหลีกเลี่ยง query ที่ยาวเกินไป
            batch_size = 1000
            existing_ids_list = []
            
            for i in range(0, len(quotation_nums), batch_size):
                batch = quotation_nums[i:i + batch_size]
                placeholders = ','.join(['%s'] * len(batch))
                query = f"SELECT {pk_column} FROM {table_name} WHERE {pk_column} IN ({placeholders})"
                params = [(str(qnum),) for qnum in batch]
                
                with target_engine.connect() as conn:
                    batch_existing = pd.read_sql(query, conn, params=params)
                    existing_ids_list.append(batch_existing)
            
            existing_ids = pd.concat(existing_ids_list, ignore_index=True) if existing_ids_list else pd.DataFrame(columns=[pk_column])
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

    # ✅ Batch Insert new rows - ใช้ to_sql แทน row-by-row
    if not df_to_insert.empty:
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} rows with null {pk_column}")

        # ทำความสะอาดข้อมูลก่อน insert
        df_to_insert_valid = df_to_insert_valid.where(pd.notnull(df_to_insert_valid), None).infer_objects(copy=False)
        df_to_insert_valid = df_to_insert_valid.replace([np.inf, -np.inf], None)

        # ✅ ใช้ to_sql สำหรับ batch insert
        if not df_to_insert_valid.empty:
            try:
                print(f"💾 เริ่ม insert ข้อมูล {len(df_to_insert_valid)} rows...")
                
                # ใช้ to_sql แทน row-by-row processing
                df_to_insert_valid.to_sql(
                    table_name, 
                    target_engine, 
                    if_exists='append', 
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                
                print(f"✅ Insert สำเร็จ {len(df_to_insert_valid)} rows")
            except Exception as e:
                print(f"❌ เกิดข้อผิดพลาดในการ insert: {e}")
                # Fallback to individual inserts if to_sql fails
                print("🔄 ลองใช้วิธี insert แบบ individual...")
                with target_engine.begin() as conn:
                    records = df_to_insert_valid.to_dict(orient='records')
                    for i, record in enumerate(records):
                        if i % 1000 == 0:
                            print(f"📊 Inserted {i}/{len(records)} rows...")
                        stmt = pg_insert(metadata).values(**record)
                        stmt = stmt.on_conflict_do_nothing(index_elements=[pk_column])
                        conn.execute(stmt)
                print(f"✅ Insert สำเร็จ {len(records)} rows (fallback method)")

    # ✅ Batch Update existing rows - ใช้ to_sql แทน row-by-row
    if not df_to_update.empty:
        # ดึงข้อมูลเดิมสำหรับเปรียบเทียบแบบ batch
        if len(common_quotation_nums) > 0:
            try:
                # แบ่งเป็น batches เล็กๆ
                batch_size = 1000
                df_existing_list = []
                
                for i in range(0, len(common_quotation_nums), batch_size):
                    batch = list(common_quotation_nums)[i:i + batch_size]
                    update_placeholders = ','.join(['%s'] * len(batch))
                    update_query = f"SELECT * FROM {table_name} WHERE {pk_column} IN ({update_placeholders})"
                    update_params = [(str(qnum),) for qnum in batch]
                    
                    with target_engine.connect() as conn:
                        batch_existing = pd.read_sql(update_query, conn, params=update_params)
                        df_existing_list.append(batch_existing)
                
                df_existing_for_update = pd.concat(df_existing_list, ignore_index=True) if df_existing_list else pd.DataFrame()
                print(f"✅ ดึงข้อมูลเดิมสำเร็จ {len(df_existing_for_update)} rows")
            except Exception as e:
                print(f"⚠️ เกิดข้อผิดพลาดในการดึงข้อมูลเดิม: {e}")
                df_existing_for_update = pd.DataFrame()
        else:
            df_existing_for_update = pd.DataFrame()
        
        # ✅ เปรียบเทียบข้อมูลแบบ vectorized แทน row-by-row
        if not df_existing_for_update.empty:
            exclude_columns = [pk_column, 'agent_id', 'customer_id', 'car_id', 'sales_id',
                               'order_type_id', 'payment_plan_id', 'create_at', 'update_at']
            
            compare_cols = [col for col in df.columns if col not in exclude_columns]
            
            # ใช้ merge แทน iterrows
            merged = df_to_update.merge(df_existing_for_update, on=pk_column, suffixes=('_new', '_old'))
            
            # ✅ เปรียบเทียบแบบ vectorized
            changed_mask = pd.Series([False] * len(merged), index=merged.index)
            
            for col in compare_cols:
                col_new = f"{col}_new"
                col_old = f"{col}_old"
                
                if col_new in merged.columns and col_old in merged.columns:
                    # เปรียบเทียบแบบ vectorized
                    mask = (merged[col_new] != merged[col_old]) | (merged[col_new].isna() != merged[col_old].isna())
                    changed_mask = changed_mask | mask
            
            # ดึงแถวที่มีการเปลี่ยนแปลง
            changed_rows_df = merged[changed_mask][[pk_column] + compare_cols].copy()
            
            if not changed_rows_df.empty:
                print(f"🔄 Updating {len(changed_rows_df)} changed rows...")
                
                # ทำความสะอาดข้อมูล
                changed_rows_df = changed_rows_df.where(pd.notnull(changed_rows_df), None)
                changed_rows_df = changed_rows_df.replace([np.inf, -np.inf], None)
                
                # ✅ ใช้ to_sql สำหรับ batch update
                try:
                    # สร้าง temporary table สำหรับ update
                    temp_table_name = f"temp_update_{table_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    
                    # Insert ข้อมูลใหม่ลง temporary table
                    changed_rows_df.to_sql(
                        temp_table_name,
                        target_engine,
                        if_exists='replace',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    
                    # ใช้ SQL UPDATE แบบ batch
                    update_columns = ', '.join([f"{col} = t.{col}" for col in compare_cols])
                    update_sql = f"""
                        UPDATE {table_name} 
                        SET {update_columns}, update_at = NOW()
                        FROM {temp_table_name} t
                        WHERE {table_name}.{pk_column} = t.{pk_column}
                    """
                    
                    with target_engine.begin() as conn:
                        conn.execute(update_sql)
                    
                    # ลบ temporary table
                    with target_engine.begin() as conn:
                        conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                    
                    print(f"✅ Update สำเร็จ {len(changed_rows_df)} rows")
                except Exception as e:
                    print(f"❌ เกิดข้อผิดพลาดในการ update: {e}")
                    # Fallback to individual updates
                    print("🔄 ลองใช้วิธี update แบบ individual...")
                    with target_engine.begin() as conn:
                        for i, (_, row) in enumerate(changed_rows_df.iterrows()):
                            if i % 1000 == 0:
                                print(f"📊 Updated {i}/{len(changed_rows_df)} rows...")
                            record = row.to_dict()
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
                    print(f"✅ Update สำเร็จ {len(changed_rows_df)} rows (fallback method)")
            else:
                print("✅ No changes detected for update.")
        else:
            print("✅ No existing data to update.")

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