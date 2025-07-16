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
        WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df_pay = pd.read_sql(query_pay, source_engine)

    query_plan = """
        SELECT quo_num, idcar, carprovince, camera, no_car, brandplan, seriesplan, sub_seriesplan,
               yearplan, detail_car, vehGroup, vehBodyTypeDesc, seatingCapacity,
               weight_car, cc_car, color_car, datestart
        FROM fin_system_select_plan
        WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
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
    df_cleaned['seat_count'] = pd.to_numeric(df_cleaned['seat_count'], errors='coerce')
    df_cleaned['seat_count'] = df_cleaned['seat_count'].astype('Int64')

    # ✅ เพิ่ม: ถ้าข้อมูลเป็น "-", ".", "...", "none" → แปลงเป็น None
    pattern_to_none = r'^[-\.]+$|^(?i:none)$'
    df_cleaned = df_cleaned.replace(pattern_to_none, np.nan, regex=True)

    return df_cleaned

@op
def load_car_data(df: pd.DataFrame):
    table_name = 'dim_car'
    pk_column = 'quotation_num'

    insp = inspect(target_engine)
    columns = [col['name'] for col in insp.get_columns(table_name)]
    if 'quotation_num' not in columns:
        with target_engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE {table_name} ADD COLUMN quotation_num varchar;'))
        print("✅ Added column 'quotation_num'")

    metadata = Table(table_name, MetaData(), autoload_with=target_engine)
    records = df.to_dict(orient='records')

    chunk_size = 5000
    for start in range(0, len(records), chunk_size):
        end = start + chunk_size
        chunk = records[start:end]
        print(f"🔄 Upserting chunk {start // chunk_size + 1}: records {start} to {end - 1}")

        with target_engine.begin() as conn:
            for record in chunk:
                if not record.get('quotation_num'):
                    continue
                stmt = pg_insert(metadata).values(**record)
                update_columns = {c.name: stmt.excluded[c.name] for c in metadata.columns if c.name != pk_column}
                stmt = stmt.on_conflict_do_update(index_elements=[pk_column], set_=update_columns)
                conn.execute(stmt)

    print("✅ Upsert completed successfully.")

@job
def dim_car_etl():
    load_car_data(clean_car_data(extract_car_data()))

if __name__ == "__main__":
    df_raw = extract_car_data()
    print("✅ Extracted:", df_raw.shape)
    print(df_raw.head(3))

    df_clean = clean_car_data(df_raw)
    print("✅ Cleaned columns:", df_clean.columns)

    print(df_clean.head(10))

    # output_path = "cleaned_dim_car.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    # load_car_data(df_clean)
    # print("🎉 Test completed! Data upserted to dim_car.")
