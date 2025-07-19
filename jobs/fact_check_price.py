from dagster import op, job
import pandas as pd
import numpy as np
import os
import json
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

load_dotenv()

# DB: Source (MariaDB)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)

# DB: Target (PostgreSQL)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_fact_check_price():
    df_logs = pd.read_sql("""
        SELECT cuscode, brand, series, subseries, year, no_car, type, repair_type,
               assured_insurance_capital1, camera, addon, quo_num, create_at, results, selected, carprovince
        FROM fin_customer_logs_B2B
    """, source_engine, chunksize=10000)

    df_checkprice = pd.read_sql("""
        SELECT id_cus, datekey, brand, model, submodel, yearcar, idcar, nocar, type_ins,
               company, tunprakan, deduct, status, type_driver, type_camera, type_addon, status_send
        FROM fin_checkprice
    """, source_engine, chunksize=10000)

    return df_logs, df_checkprice

@op
def clean_fact_check_price(data):
    df_logs, df_checkprice = data

    province_list = ["กรุงเทพมหานคร", "กระบี่", "กาญจนบุรี", "กาฬสินธุ์", "กำแพงเพชร",
    "ขอนแก่น", "จันทบุรี", "ฉะเชิงเทรา", "ชลบุรี", "ชัยนาท", "ชัยภูมิ",
    "ชุมพร", "เชียงใหม่", "เชียงราย", "ตรัง", "ตราด", "ตาก", "นครนายก",
    "นครปฐม", "นครพนม", "นครราชสีมา", "นครศรีธรรมราช", "นครสวรรค์",
    "นนทบุรี", "นราธิวาส", "น่าน", "บึงกาฬ", "บุรีรัมย์", "ปทุมธานี",
    "ประจวบคีรีขันธ์", "ปราจีนบุรี", "ปัตตานี", "พระนครศรีอยุธยา",
    "พังงา", "พัทลุง", "พิจิตร", "พิษณุโลก", "เพชรบุรี", "เพชรบูรณ์",
    "แพร่", "พะเยา", "ภูเก็ต", "มหาสารคาม", "มุกดาหาร", "แม่ฮ่องสอน",
    "ยะลา", "ยโสธร", "ระนอง", "ระยอง", "ราชบุรี", "ร้อยเอ็ด", "ลพบุรี",
    "ลำปาง", "ลำพูน", "เลย", "ศรีสะเกษ", "สกลนคร", "สงขลา", "สตูล",
    "สมุทรปราการ", "สมุทรสงคราม", "สมุทรสาคร", "สระแก้ว", "สระบุรี",
    "สิงห์บุรี", "สุโขทัย", "สุพรรณบุรี", "สุราษฎร์ธานี", "สุรินทร์",
    "หนองคาย", "หนองบัวลำภู", "อ่างทอง", "อุดรธานี", "อุทัยธานี",
    "อุตรดิตถ์", "อุบลราชธานี", "อำนาจเจริญ"]  

    def extract_company_names(x):
        if pd.isnull(x): return [None]*4
        data = json.loads(x)
        names = [d.get('company_name') for d in data if isinstance(d, dict)] if isinstance(data, list) else [data.get('company_name')]
        return (names + [None]*4)[:4]

    def extract_selected(x):
        if pd.isnull(x): return None
        data = json.loads(x)
        if isinstance(data, list) and data: return data[0].get('company_name')
        if isinstance(data, dict): return data.get('company_name')
        return None

    logs = df_logs.copy()
    logs[['company_name1', 'company_name2', 'company_name3', 'company_name4']] = logs['results'].apply(extract_company_names).apply(pd.Series)
    logs['selecteds'] = logs['selected'].apply(extract_selected)

    logs['camera'] = logs['camera'].map({'yes': 'มีกล้องหน้ารถ', 'no': 'ไม่มีกล้องหน้ารถ'})
    logs['addon'] = logs['addon'].map({'ไม่มี': 'ไม่มีการต่อเติม'})

    logs.rename(columns={
        'cuscode': 'id_cus', 'brand': 'brand', 'series': 'model', 'subseries': 'submodel',
        'year': 'yearcar', 'no_car': 'car_code', 'assured_insurance_capital1': 'sum_insured',
        'camera': 'type_camera', 'addon': 'type_addon', 'quo_num': 'select_quotation',
        'create_at': 'transaction_date', 'carprovince': 'province_car'
    }, inplace=True)

    logs['type_insurance'] = 'ชั้น' + logs['type'].astype(str) + logs['repair_type'].astype(str)
    logs['input_type'] = 'auto'
    logs.drop(columns=['type', 'repair_type', 'results', 'selected'], inplace=True)

    def split_company(x):
        if pd.isnull(x): return [None]*4
        parts = [i.strip() for i in x.split('/')]
        return (parts + [None]*4)[:4]

    def extract_plate(x):
        if pd.isnull(x): return None
        x = str(x)
        for p in province_list:
            x = x.replace(p, '')
        return re.findall(r'\d{1,2}[ก-ฮ]{1,2}\d{1,4}', x.replace('-', '').replace('/', ''))[0] if re.findall(r'\d{1,2}[ก-ฮ]{1,2}\d{1,4}', x) else None

    check = df_checkprice.copy()
    check[['company_name1', 'company_name2', 'company_name3', 'company_name4']] = check['company'].apply(split_company).apply(pd.Series)
    check['id_car'] = check['idcar'].apply(extract_plate)
    check['province_car'] = check['idcar'].apply(lambda x: next((p for p in province_list if p in str(x)), None))
    check.drop(columns=['company', 'idcar'], inplace=True)

    check.rename(columns={
        'datekey': 'transaction_date', 'nocar': 'car_code', 'type_ins': 'type_insurance',
        'tunprakan': 'sum_insured', 'deduct': 'deductible'
    }, inplace=True)
    check['input_type'] = 'manual'

    df_combined = pd.concat([logs, check], ignore_index=True, sort=False)
    df_combined = df_combined.replace(r'^\s*$', np.nan, regex=True).applymap(lambda x: x.strip() if isinstance(x, str) else x)

    df_combined = df_combined[~df_combined['id_cus'].isin([
        'FIN-TestApp', 'FIN-TestApp2', 'FIN-TestApp3', 'FIN-TestApp6', 'FIN-TestApp-2025', 'FNGtest', '????♡☆umata♡☆??'
    ])]

    df_combined['model'] = df_combined['model'].apply(lambda val: re.sub(r'^[\W_]+', '', str(val).strip()) if pd.notnull(val) else None)
    # Convert transaction_date to YYYYMMDD int
    df_combined['transaction_date'] = pd.to_datetime(df_combined['transaction_date'], errors='coerce')
    df_combined['transaction_date'] = df_combined['transaction_date'].dt.strftime('%Y%m%d').astype('Int64')
    df_combined = df_combined.drop_duplicates()

    return df_combined

@op
def load_fact_check_price(df: pd.DataFrame):
    table_name = 'fact_check_price'
    pk_column = ['id_cus', 'brand', 'model', 'submodel', 'yearcar', 'car_code',
                'sum_insured', 'type_camera', 'type_addon', 'transaction_date',
                'province_car', 'company_name1', 'company_name2', 'company_name3',
                'company_name4', 'selecteds', 'type_insurance', 'id_car']

    # ✅ กรอง car_id ซ้ำจาก DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()

    # ✅ Load ข้อมูลเดิมจาก PostgreSQL
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    # ✅ กรอง car_id ซ้ำจากข้อมูลเก่า
    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    # ✅ Identify car_id ใหม่ (ไม่มีใน DB)
    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    # ✅ Identify car_id ที่มีอยู่แล้ว
    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    # ✅ Merge ด้วย suffix (_new, _old)
    merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

    # ✅ ระบุคอลัมน์ที่ใช้เปรียบเทียบ (ยกเว้น key และ audit fields)
    exclude_columns = [pk_column, 'check_price_id', 'create_at', 'update_at']
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

    # ✅ เตรียม DataFrame สำหรับ update โดยใช้ car_id ปกติ (ไม่เติม _new)
    update_cols = [f"{col}_new" for col in compare_cols]
    all_cols = [pk_column] + update_cols

    df_diff_renamed = df_diff[all_cols].copy()
    df_diff_renamed.columns = [pk_column] + compare_cols  # เปลี่ยนชื่อ column ให้ตรงกับตารางจริง

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

    # ✅ Load table metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert (กรอง car_id ที่เป็น NaN)
    if not df_to_insert.empty:
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} insert rows with null car_id")
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
def fact_check_price_etl():
    load_fact_check_price(clean_fact_check_price(extract_fact_check_price()))

if __name__ == "__main__":
    df_logs, df_checkprice = extract_fact_check_price()
    print("✅ Extracted logs:", df_logs.shape)
    print("✅ Extracted checkprice:", df_checkprice.shape)

    df_clean = clean_fact_check_price((df_logs, df_checkprice))
    print("✅ Cleaned columns:", df_clean.columns)

    # print(df_clean.head(10))

    # output_path = "fact_check_price.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_fact_check_price(df_clean)
    print("🎉 Test completed! Data upserted to dim_car.")
