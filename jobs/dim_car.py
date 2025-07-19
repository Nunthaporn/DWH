from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ✅ Load .env
load_dotenv()

# ✅ DB source (MariaDB)
source_user = os.getenv('DB_USER')
source_password = os.getenv('DB_PASSWORD')
source_host = os.getenv('DB_HOST')
source_port = os.getenv('DB_PORT')
source_db = 'fininsurance'

source_engine = create_engine(
    f"mysql+pymysql://{source_user}:{source_password}@{source_host}:{source_port}/{source_db}"
)

# ✅ DB target (PostgreSQL)
target_user = os.getenv('DB_USER_test')
target_password = os.getenv('DB_PASSWORD_test')
target_host = os.getenv('DB_HOST_test')
target_port = os.getenv('DB_PORT_test')
target_db = 'fininsurance'

target_engine = create_engine(
    f"postgresql+psycopg2://{target_user}:{target_password}@{target_host}:{target_port}/{target_db}"
)

@op
def extract_car_data():
    query_pay = """
        SELECT quo_num, id_motor1, id_motor2, datestart
        FROM fin_system_pay
        WHERE datestart >= '2025-01-01' AND datestart < '2025-07-01'
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df_pay = pd.read_sql(query_pay, source_engine)

    query_plan = """
        SELECT quo_num, idcar, carprovince, camera, no_car, brandplan, seriesplan, sub_seriesplan,
               yearplan, detail_car, vehGroup, vehBodyTypeDesc, seatingCapacity,
               weight_car, cc_car, color_car, datestart
        FROM fin_system_select_plan
        WHERE datestart >= '2025-01-01' AND datestart < '2025-07-01'
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df_plan = pd.read_sql(query_plan, source_engine)

    df_merged = pd.merge(df_pay, df_plan, on='quo_num', how='left')
    return df_merged

@op
def clean_car_data(df: pd.DataFrame):
    df = df.drop_duplicates(subset=['id_motor2'])
    df = df.drop_duplicates(subset=['idcar'])
    df = df.drop(columns=['datestart_x', 'datestart_y'], errors='ignore')

    rename_columns = {
        "quo_num": "quotation_num",
        "id_motor2": "car_id",
        "id_motor1": "engine_number",
        "idcar": "car_registration",
        "carprovince": "car_province",
        "camera": "camera",
        "no_car": "car_no",
        "brandplan": "car_brand",
        "seriesplan": "car_series",
        "sub_seriesplan": "car_subseries",
        "yearplan": "car_year",
        "detail_car": "car_detail",
        "vehGroup": "vehicle_group",
        "vehBodyTypeDesc": "vehbodytypedesc",
        "seatingCapacity": "seat_count",
        "weight_car": "vehicle_weight",
        "cc_car": "engine_capacity",
        "color_car": "vehicle_color"
    }

    df = df.rename(columns=rename_columns)
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)

    valid_mask = df['car_id'].astype(str).str.strip().ne('') & df['car_id'].notna()
    df_with_id = df[valid_mask]
    df_without_id = df[~valid_mask]
    df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='car_id', keep='first')
    df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)
    df_cleaned = df_cleaned.drop(columns=['non_empty_count'])

    df_cleaned.columns = df_cleaned.columns.str.lower()

    df_cleaned['seat_count'] = df_cleaned['seat_count'].replace("อื่นๆ", np.nan)
    df_cleaned['seat_count'] = pd.to_numeric(df_cleaned['seat_count'], errors='coerce')

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

    def extract_clean_plate(value):
        if pd.isnull(value) or value.strip() == "":
            return None
        text = re.split(r'[\/]', value.strip())[0].split()[0]
        for prov in province_list:
            if prov in text:
                text = text.replace(prov, "").strip()
        reg_match = re.match(r'^((?:\d{1,2})?[ก-ฮ]{1,3}\d{1,4})', text)
        if reg_match:
            final_plate = reg_match.group(1).replace('-', '')
            match_two_digits = re.match(r'^(\d{2})([ก-ฮ].*)$', final_plate)
            if match_two_digits:
                final_plate = match_two_digits.group(1)[1:] + match_two_digits.group(2)
            if final_plate.startswith("0"):
                final_plate = final_plate[1:]
            return final_plate
        else:
            return None

    df_cleaned['car_registration'] = df_cleaned['car_registration'].apply(extract_clean_plate)

    # ✅ ปรับ pattern ใหม่: รองรับ "-", ".", "none", "NaN", "UNDEFINE", "undefined"
    pattern_to_none = r'^[-\.]+$|^(?i:none|nan|undefine|undefined)$'
    df_cleaned = df_cleaned.replace(pattern_to_none, np.nan, regex=True)

    # ✅ ทำความสะอาด engine_number ให้มีเฉพาะ A-Z, a-z, 0-9
    def clean_engine_number(value):
        if pd.isnull(value):
            return None
        cleaned = re.sub(r'[^A-Za-z0-9]', '', str(value))
        return cleaned if cleaned else None

    df_cleaned['engine_number'] = df_cleaned['engine_number'].apply(clean_engine_number)

    # ✅ ทำความสะอาด car_province ให้มีเฉพาะจังหวัด
    def clean_province(value):
        if pd.isnull(value):
            return None
        value = str(value).strip()
        if value in province_list:
            return value
        return None

    df_cleaned['car_province'] = df_cleaned['car_province'].apply(clean_province)
    df_cleaned = df_cleaned.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df_cleaned['car_no'] = df_cleaned['car_no'].replace("ไม่มี", np.nan)
    df_cleaned['car_brand'] = df_cleaned['car_brand'].replace("-", np.nan)

    def has_thai_chars(value):
        if pd.isnull(value):
            return False
        return bool(re.search(r'[ก-๙]', str(value)))

    df_cleaned = df_cleaned[~df_cleaned['car_id'].apply(has_thai_chars)]
    
    series_noise_pattern = r"^[-–_\.\/\+].*|^<=200CC$|^>250CC$|^‘NQR 75$"

    df_cleaned['car_series'] = df_cleaned['car_series'].replace(series_noise_pattern, np.nan, regex=True)
    df_cleaned['car_subseries'] = df_cleaned['car_subseries'].replace(series_noise_pattern, np.nan, regex=True)

    def remove_leading_vowels(value):
        if pd.isnull(value):
            return value
        # ลบสระและเครื่องหมายกำกับไทยต้นข้อความ
        return re.sub(r"^[\u0E30-\u0E39\u0E47-\u0E4E\u0E3A]+", "", value.strip())

    df_cleaned['car_series'] = df_cleaned['car_series'].apply(remove_leading_vowels)
    df_cleaned['car_subseries'] = df_cleaned['car_subseries'].apply(remove_leading_vowels)

    def clean_engine_capacity(value):
        if pd.isnull(value):
            return None
        value = str(value).strip()
        # ลบจุดซ้ำ เช่น "1..2" → "1.2"
        value = re.sub(r'\.{2,}', '.', value)
        # พยายามแปลงเป็น float
        try:
            float_val = float(value)
            return float_val
        except ValueError:
            return None

    df_cleaned['engine_capacity'] = df_cleaned['engine_capacity'].apply(clean_engine_capacity)

    def clean_float_only(value):
        if pd.isnull(value):
            return None
        value = str(value).strip()
        value = re.sub(r'\.{2,}', '.', value)
        if value in ['', '.', '-']:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    df_cleaned['engine_capacity'] = df_cleaned['engine_capacity'].apply(clean_float_only)
    df_cleaned['vehicle_weight'] = df_cleaned['vehicle_weight'].apply(clean_float_only)
    df_cleaned['seat_count'] = df_cleaned['seat_count'].apply(lambda x: int(clean_float_only(x)) if clean_float_only(x) is not None else None)
    df_cleaned['car_year'] = df_cleaned['car_year'].apply(lambda x: int(clean_float_only(x)) if clean_float_only(x) is not None else None)

    df_cleaned['seat_count'] = pd.to_numeric(df_cleaned['seat_count'], errors='coerce').astype('Int64')
    df_cleaned['car_year'] = pd.to_numeric(df_cleaned['car_year'], errors='coerce').astype('Int64')
    df_cleaned = df_cleaned.replace(r'NaN', np.nan, regex=True)
    df_cleaned = df_cleaned.drop_duplicates()

    return df_cleaned

# @op
# def load_car_data(df: pd.DataFrame):
#     table_name = 'dim_car'
#     pk_column = 'car_id'

#     insp = inspect(target_engine)
#     columns = [col['name'] for col in insp.get_columns(table_name)]
#     if 'quotation_num' not in columns:
#         with target_engine.begin() as conn:
#             conn.execute(text(f'ALTER TABLE {table_name} ADD COLUMN quotation_num varchar;'))
#         print("✅ Added column 'quotation_num'")

#     metadata = Table(table_name, MetaData(), autoload_with=target_engine)
#     records = df.to_dict(orient='records')

#     chunk_size = 50000
#     for start in range(0, len(records), chunk_size):
#         end = start + chunk_size
#         chunk = records[start:end]
#         print(f"🔄 Upserting chunk {start // chunk_size + 1}: records {start} to {end - 1}")

#         with target_engine.begin() as conn:
#             for record in chunk:
#                 if not record.get('quotation_num'):
#                     continue
#                 stmt = pg_insert(metadata).values(**record)
#                 update_columns = {c.name: stmt.excluded[c.name] for c in metadata.columns if c.name != pk_column}
#                 stmt = stmt.on_conflict_do_update(index_elements=[pk_column], set_=update_columns)
#                 conn.execute(stmt)

#     print("✅ Upsert completed successfully.")

@op
def load_car_data(df: pd.DataFrame):
    table_name = 'dim_car'
    pk_column = 'car_id'

    # ✅ กรองซ้ำก่อน set_index เพื่อหลีกเลี่ยง duplicated index
    df = df[~df[pk_column].duplicated(keep='first')].copy()

    # ✅ Load ข้อมูลเดิมจาก DB
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    # ✅ กรองซ้ำจาก DB
    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    # ✅ Identify: car_id ใหม่
    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    # ✅ Identify: car_id ที่มีอยู่แล้ว
    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    # ✅ Merge เพื่อตรวจสอบความแตกต่าง
    merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

    # ✅ เลือกเฉพาะคอลัมน์ที่มีในทั้งสองฝั่ง (ไม่รวม key)
    exclude_columns = [pk_column, 'car_sk', 'create_at', 'update_at']
    compare_cols = [col for col in df.columns if col not in exclude_columns and f"{col}_new" in merged.columns and f"{col}_old" in merged.columns]

    # ✅ ตรวจหาความแตกต่าง
    def is_different(row):
        for col in compare_cols:
            if pd.isna(row[f"{col}_new"]) and pd.isna(row[f"{col}_old"]):
                continue
            if row[f"{col}_new"] != row[f"{col}_old"]:
                return True
        return False

    df_diff = merged[merged.apply(is_different, axis=1)].copy()

    # ✅ เตรียมเฉพาะข้อมูลใหม่สำหรับ update
    update_records = df_diff[[f"{col}_new" for col in [pk_column] + compare_cols]].copy()
    update_records.columns = [pk_column] + compare_cols

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(update_records)} rows")

    # ✅ Metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert ใหม่
    if not df_to_insert.empty:
        with target_engine.begin() as conn:
            conn.execute(metadata.insert(), df_to_insert.to_dict(orient='records'))

    # ✅ Update ที่เปลี่ยน
    if not update_records.empty:
        with target_engine.begin() as conn:
            for record in update_records.to_dict(orient='records'):
                stmt = pg_insert(metadata).values(**record)
                update_columns = {c.name: stmt.excluded[c.name] for c in metadata.columns if c.name != pk_column}
                stmt = stmt.on_conflict_do_update(index_elements=[pk_column], set_=update_columns)
                conn.execute(stmt)

    print("✅ Insert/update completed.")

@job
def dim_car_etl():
    load_car_data(clean_car_data(extract_car_data()))

if __name__ == "__main__":
    df_raw = extract_car_data()
    print("✅ Extracted:", df_raw.shape)
    # print(df_raw.head(3))

    df_clean = clean_car_data(df_raw)
    print("✅ Cleaned columns:", df_clean.columns)

    # print(df_clean.head(10))

    # output_path = "cleaned_dim_car.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_car_data(df_clean)
    print("🎉 Test completed! Data upserted to dim_car.")
