# %%
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, update, text
import numpy as np
from datetime import datetime, timedelta
import re

# โหลดตัวแปรจาก .env
load_dotenv()

# ตั้งค่าการเชื่อมต่อฐานข้อมูล
def get_mariadb_engine():
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')  
    database = 'fininsurance'
    return create_engine(f'mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}')

def get_mariadb_task_engine():
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')  
    database = 'fininsurance_task'
    return create_engine(f'mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}')

def get_postgres_engine():
    user = os.getenv('DB_USER_test')
    password = os.getenv('DB_PASSWORD_test')
    host = os.getenv('DB_HOST_test')
    port = os.getenv('DB_PORT_test')  
    database = 'fininsurance'
    return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

# %%
def update_fact_table(engine, table_name, records, id_column, chunk_size=5000):
    """ฟังก์ชันสำหรับอัปเดต fact_sales_quotation table"""
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    
    for start in range(0, len(records), chunk_size):
        end = start + chunk_size
        chunk = records[start:end]

        print(f"🔄 Updating chunk {start // chunk_size + 1}: records {start} to {end - 1}")

        with engine.begin() as conn:
            for record in chunk:
                if 'quotation_num' not in record or pd.isna(record['quotation_num']):
                    print(f"⚠️ Skip row: no quotation_num: {record}")
                    continue
                if id_column not in record or pd.isna(record[id_column]):
                    print(f"⚠️ Skip row: no {id_column}: {record}")
                    continue

                stmt = (
                    update(table)
                    .where(table.c.quotation_num == record['quotation_num'])
                    .values(**{id_column: record[id_column]})
                )
                conn.execute(stmt)

    print(f"✅ Update {id_column} completed successfully.")

# %%
def merge_agent_to_quotation():
    """รวมข้อมูล Agent เข้ากับ Quotation"""
    print("🔄 Starting Agent merge process...")
    
    # ดึงข้อมูลจาก wp_users
    engine = get_mariadb_engine()
    query = """
    SELECT cuscode, name, rank,
           CASE 
           WHEN user_registered = '0000-00-00 00:00:00.000' THEN '2000-01-01 00:00:00'
             ELSE user_registered 
           END AS user_registered,
           status, fin_new_group, fin_new_mem,
           type_agent, typebuy, user_email, name_store, address, city, district,
           province, province_cur, area_cur, postcode, tel,date_active
    FROM wp_users
    """
    df = pd.read_sql(query, engine)
    df['user_registered'] = pd.to_datetime(df['user_registered'].astype(str), errors='coerce')

    # ดึงข้อมูลจาก policy_register
    query = """
    SELECT cuscode , career
    FROM policy_register
    """
    df1 = pd.read_sql(query, engine)
    
    # รวมข้อมูล
    df_merged = pd.merge(df, df1, on='cuscode', how='left')
    
    # รวม agent_region
    def combine_columns(a, b):
        a_str = str(a).strip() if pd.notna(a) else ''
        b_str = str(b).strip() if pd.notna(b) else ''
        
        if a_str == '' and b_str == '':
            return ''
        elif a_str == '':
            return b_str
        elif b_str == '':
            return a_str
        elif a_str == b_str:
            return a_str
        else:
            return f"{a_str} + {b_str}"

    df_merged['agent_region'] = df_merged.apply(lambda row: combine_columns(row['fin_new_group'], row['fin_new_mem']), axis=1)
    df_merged = df_merged.drop(columns=['fin_new_group','fin_new_mem'])
    df_merged['date_active'] = pd.to_datetime(df_merged['date_active'], errors='coerce')

    # ตรวจสอบสถานะ agent
    now = pd.Timestamp.now()
    one_month_ago = now - pd.DateOffset(months=1)

    def check_condition(row):
        if row['status'] == 'defect':
            return 'inactive'
        elif pd.notnull(row['date_active']) and row['date_active'] < one_month_ago:
            return 'inactive'
        else:
            return 'active'

    df_merged['status_agent'] = df_merged.apply(check_condition, axis=1)
    df_merged = df_merged.drop(columns=['status','date_active'])

    # เปลี่ยนชื่อคอลัมน์
    rename_columns = {
        "cuscode": "agent_id",
        "name": "agent_name",
        "rank": "agent_rank",
        "user_registered": "hire_date",
        "status_agent": "status_agent",
        "type_agent": "type_agent",
        "typebuy": "is_experienced",
        "user_email": "agent_email",
        "name_store": "store_name",
        "address": "agent_address",
        "city": "subdistrict",
        "district": "district",
        "province": "province",
        "province_cur": "current_province",
        "area_cur": "current_area",
        "postcode": "zipcode",
        "tel": "mobile_number",
        "career": "job",
        "agent_region": "agent_region"
    }
    df = df_merged.rename(columns=rename_columns)

    # แก้ไขข้อมูล
    df['is_experienced_fix'] = df['is_experienced'].apply(lambda x: 'เคยขาย' if str(x).strip().lower() == 'ไม่เคยขาย' else 'ไม่เคยขาย')
    df = df.drop(columns=['is_experienced'])
    df.rename(columns={'is_experienced_fix': 'is_experienced'}, inplace=True)

    # ตรวจสอบ type_agent และ agent_rank
    valid_types = ['BUY', 'SELL', 'SHARE']
    df.loc[~df['type_agent'].isin(valid_types), 'type_agent'] = np.nan

    valid_rank = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    df.loc[~df['agent_rank'].isin(valid_rank), 'agent_rank'] = np.nan

    # ทำความสะอาดที่อยู่
    def clean_address(addr):
        if pd.isna(addr):
            return ''
        addr = re.sub(r'(เลขที่|หมู่ที่|หมู่บ้าน|ซอย|ถนน)[\s\-]*', '', addr, flags=re.IGNORECASE)
        addr = re.sub(r'\s*-\s*', '', addr)
        addr = re.sub(r'\s+', ' ', addr)
        return addr.strip()

    df['agent_address_cleaned'] = df['agent_address'].apply(clean_address)
    df = df.drop(columns=['agent_address'])
    df.rename(columns={'agent_address_cleaned': 'agent_address'}, inplace=True)

    # ลบข้อมูลซ้ำ
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)
    valid_agent_id_mask = df['agent_id'].astype(str).str.strip().ne('') & df['agent_id'].notna()
    df_with_id = df[valid_agent_id_mask]
    df_without_id = df[~valid_agent_id_mask]
    df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='agent_id', keep='first')
    df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)
    df_cleaned = df_cleaned.drop(columns=['non_empty_count'])
    df_cleaned = df_cleaned.replace(to_replace=r'^\s*$|(?i:^none$)|^-$', value=np.nan, regex=True)
    df_cleaned.columns = df_cleaned.columns.str.lower()

    # แก้ไขข้อมูลเพิ่มเติม
    df_cleaned['is_experienced'] = df_cleaned['is_experienced'].apply(lambda x: 'yes' if str(x).strip().lower() == 'no' else 'no')
    df_cleaned = df_cleaned.replace(r'^\.$', np.nan, regex=True)

    def clean_value(val):
        if pd.isna(val):
            return None
        if isinstance(val, str):
            if val.strip() == "":
                return None
            if val.strip().lower() == "nan":
                return None
        return val

    df_cleaned = df_cleaned.applymap(clean_value)

    # ดึงข้อมูล quotation และ agent mapping
    query = """
    SELECT quo_num, id_cus
    FROM fin_system_select_plan
    """
    df4 = pd.read_sql(query, engine)
    df4 = df4.rename(columns={'quo_num': 'quotation_num'})

    # ดึงข้อมูลจาก PostgreSQL
    pg_engine = get_postgres_engine()
    query = """
    SELECT *
    FROM fact_sales_quotation 
    """
    df6 = pd.read_sql(query, pg_engine)
    df6 = df6.drop(columns=['create_at', 'update_at', 'agent_id'])

    df_result1 = pd.merge(df4, df6, on=['quotation_num'], how='right')

    query = """
    SELECT *
    FROM dim_agent 
    """
    df5 = pd.read_sql(query, pg_engine)
    df5 = df5.drop(columns=['create_at', 'update_at'])
    df5 = df5.rename(columns={'agent_id': 'id_cus'})

    df_result = pd.merge(df_result1, df5, on=['id_cus'], how='inner')
    df_result = df_result.where(pd.notnull(df_result), None)
    df_result = df_result.rename(columns={'id_contact': 'agent_id'})
    df_selected = df_result[['quotation_num', 'agent_id']]

    # อัปเดตฐานข้อมูล
    update_fact_table(pg_engine, 'fact_sales_quotation', df_selected.to_dict(orient='records'), 'agent_id')
    
    print("✅ Agent merge completed!")

# %%
def merge_car_to_quotation():
    """รวมข้อมูล Car เข้ากับ Quotation"""
    print("🔄 Starting Car merge process...")
    
    engine = get_mariadb_engine()
    
    # ดึงข้อมูลจาก fin_system_pay
    query = """
    SELECT quo_num, id_motor1, id_motor2, datestart
    FROM fin_system_pay
    WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
    AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df = pd.read_sql(query, engine)

    # ดึงข้อมูลจาก fin_system_select_plan
    query = """
    SELECT quo_num, idcar, carprovince, camera, no_car, brandplan, seriesplan, sub_seriesplan, yearplan, detail_car, vehGroup, vehBodyTypeDesc, seatingCapacity, weight_car, cc_car, color_car, datestart
    FROM fin_system_select_plan
    WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
    AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df1 = pd.read_sql(query, engine)

    # รวมข้อมูล
    df_merged = pd.merge(df, df1, on='quo_num', how='left')
    df_merged = df_merged.drop_duplicates(subset=['id_motor2'])
    df_merged = df_merged.drop_duplicates(subset=['idcar'])
    df_merged = df_merged.drop(columns=['datestart_x', 'datestart_y'])

    # เปลี่ยนชื่อคอลัมน์
    rename_columns = {
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
        "vehBodyTypeDesc": "vehBodyTypeDesc",
        "seatingCapacity": "seat_count",
        "weight_car": "vehicle_weight",
        "cc_car": "engine_capacity",
        "color_car": "vehicle_color"
    }
    df = df_merged.rename(columns=rename_columns)

    # ทำความสะอาดข้อมูล
    df = df.replace(r'^\s*$', pd.NA, regex=True)  
    df = df[df.count(axis=1) > 1]

    # ลบข้อมูลซ้ำ
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)
    valid_car_id_mask = df['car_id'].astype(str).str.strip().ne('') & df['car_id'].notna()
    df_with_id = df[valid_car_id_mask]
    df_without_id = df[~valid_car_id_mask]
    df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='car_id', keep='first')
    df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)
    df_cleaned = df_cleaned.drop(columns=['non_empty_count'])
    df_cleaned = df_cleaned.replace(to_replace=r'^\s*$|(?i:^none$)|^-$', value=np.nan, regex=True)
    df_cleaned.columns = df_cleaned.columns.str.lower()
    df_cleaned = df_cleaned.replace(r'^\.$', np.nan, regex=True)

    # แก้ไข seat_count
    df_cleaned['seat_count'] = df_cleaned['seat_count'].replace("อื่นๆ", np.nan)
    df_cleaned['seat_count'] = pd.to_numeric(df_cleaned['seat_count'], errors='coerce')

    # ทำความสะอาดทะเบียนรถ
    province_list = [
        "กรุงเทพมหานคร", "กระบี่", "กาญจนบุรี", "กาฬสินธุ์", "กำแพงเพชร",
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
        "อุตรดิตถ์", "อุบลราชธานี", "อำนาจเจริญ"
    ]

    def extract_clean_plate(value):
        if pd.isnull(value) or value.strip() == "":
            return None

        text = value.strip()
        text = re.split(r'[\/]', text)[0].strip()
        parts = text.split()
        if len(parts) > 0:
            text = parts[0].strip()
        else:
            return None

        for prov in province_list:
            if prov in text:
                text = text.replace(prov, "").strip()

        reg_match = re.match(r'^((?:\d{1,2})?[ก-ฮ]{1,3}\d{1,4})', text)
        if reg_match:
            final_plate = reg_match.group(1)
            final_plate = final_plate.replace('-', '')
            match_two_digits = re.match(r'^(\d{2})([ก-ฮ].*)$', final_plate)
            if match_two_digits:
                final_plate = match_two_digits.group(1)[1:] + match_two_digits.group(2)
            if final_plate.startswith("0"):
                final_plate = final_plate[1:]
            return final_plate
        else:
            return None

    df_cleaned['car_registration'] = df_cleaned['car_registration'].apply(extract_clean_plate)

    # ดึงข้อมูลจาก PostgreSQL
    pg_engine = get_postgres_engine()
    query = """
    SELECT *
    FROM dim_car 
    """
    df5 = pd.read_sql(query, pg_engine)
    df5 = df5.drop(columns=['create_at', 'update_at'])

    # แก้ไขข้อมูล type
    df_cleaned['car_year'] = df_cleaned['car_year'].astype('Int64')
    df5['car_year'] = df5['car_year'].astype('Int64')
    df5 = df5.replace(["NaN", "NAN", "nan"], np.nan)
    df_cleaned = df_cleaned.replace(["NaN", "NAN", "nan"], np.nan)

    fix_cols = ['vehicle_group', 'vehicle_weight', 'engine_capacity']
    for col in fix_cols:
        df_cleaned[col] = df_cleaned[col].astype(str).replace('nan', pd.NA)
        df5[col] = df5[col].astype(str).replace('nan', pd.NA)

    # รวมข้อมูล
    df_result = pd.merge(df_cleaned, df5, on=['car_id', 'car_registration'], how='right')
    df_result = df_result[['quo_num', 'car_sk']]
    df_result = df_result.rename(columns={'quo_num': 'quotation_num'})

    # ดึงข้อมูล fact_sales_quotation
    query = """
    SELECT *
    FROM fact_sales_quotation 
    """
    df6 = pd.read_sql(query, pg_engine)
    df6 = df6.drop(columns=['create_at', 'update_at', 'car_id'])

    df_result1 = pd.merge(df_result, df6, on=['quotation_num'], how='right')
    df_result1 = df_result1.rename(columns={'car_sk': 'car_id'})
    df_result1 = df_result1.drop_duplicates(subset=['quotation_num'], keep='last')
    df_result1 = df_result1.where(pd.notnull(df_result1), None)

    # อัปเดตฐานข้อมูล
    update_fact_table(pg_engine, 'fact_sales_quotation', df_result1.to_dict(orient='records'), 'car_id')
    
    print("✅ Car merge completed!")

# %%
def merge_customer_to_quotation():
    """รวมข้อมูล Customer เข้ากับ Quotation"""
    print("🔄 Starting Customer merge process...")
    
    engine = get_mariadb_engine()
    
    # ดึงข้อมูลจาก fin_system_pay
    query = """
    SELECT quo_num, address, province, amphoe, district, zipcode, datestart
    FROM fin_system_pay
    WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
    AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df = pd.read_sql(query, engine)

    # ดึงข้อมูลจาก fin_system_select_plan
    query = """
    SELECT quo_num, idcard, title, name, lastname, birthDate, career, gender, tel, email, datestart
    FROM fin_system_select_plan
    WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
    AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df1 = pd.read_sql(query, engine)

    # รวมข้อมูล
    df_merged = pd.merge(df, df1, on='quo_num', how='right')
    df_merged = df_merged.drop_duplicates(subset=['address', 'province', 'amphoe', 'district', 'zipcode'])
    df_merged = df_merged.drop_duplicates(subset=['name', 'lastname'])
    df_merged = df_merged.drop_duplicates(subset=['idcard'])
    df_merged = df_merged.drop(columns=['datestart_x', 'datestart_y'])

    # สร้าง full_name
    df_merged['full_name'] = df_merged.apply(
        lambda row: row['name'] if str(row['name']).strip() == str(row['lastname']).strip()
        else f"{str(row['name']).strip()} {str(row['lastname']).strip()}",
        axis=1
    )
    df_cleaned = df_merged.drop(columns=['name', 'lastname'])

    # คำนวณอายุ
    from datetime import date
    df_cleaned['birthDate'] = pd.to_datetime(df_cleaned['birthDate'], errors='coerce')
    df_cleaned['age'] = df_cleaned['birthDate'].apply(
        lambda x: (
            date.today().year - x.year - ((date.today().month, date.today().day) < (x.month, x.day))
            if pd.notnull(x) else pd.NA
        )
    ).astype('Int64')

    # เปลี่ยนชื่อคอลัมน์
    rename_columns = {
        'idcard': 'customer_card',
        'title': 'title',
        'full_name': 'customer_name',
        'birthDate': 'customer_dob',
        'gender': 'customer_gender',
        'tel': 'customer_telnumber',
        'email': 'customer_email',
        'address': 'address',
        'province': 'province',
        'amphoe': 'district',
        'district': 'subdistrict',
        'zipcode': 'zipcode',
        'career': 'job'
    }
    df = df_cleaned.rename(columns=rename_columns)

    # แก้ไข gender
    gender_mapping = {
        'M': 'Male',
        'F': 'Female',
    }
    df['customer_gender'] = df['customer_gender'].map(gender_mapping)

    # ทำความสะอาดข้อมูล
    df = df.replace(to_replace=r'^\s*$|^(?i:none|null|na)$|^[-.]$', value=np.nan, regex=True)
    df['customer_name'] = df['customer_name'].str.replace(r'\s*None$', '', regex=True)
    df['customer_telnumber'] = df['customer_telnumber'].str.replace('-', '', regex=False)

    # ลบข้อมูลทดสอบ
    test_names = [
        'ทดสอบ', 'ทดสอบ ', 'ทดสอบ จากฟิน', 'ทดสอบ พ.ร.บ.', 'ทดสอบ06',
        'ทดสอบ', 'ทดสอบระบบ ประกัน+พ.ร.บ.', 'ลูกค้า ทดสอบ', 'ทดสอบ เช็คเบี้ย',
        'ทดสอบพ.ร.บ. งานคีย์มือ', 'ทดสอบ ระบบ', 'ทดสอบคีย์มือ ธนชาตผู้ขับขี่',
        'ทดสอบ04', 'test', 'test2', 'test tes', 'test ระบบ', 'Tes ระบบ'
    ]
    df = df[~df['customer_name'].isin(test_names)]

    # ทำความสะอาดเบอร์โทร
    def clean_telnumber(val):
        if pd.isnull(val) or val.strip() == "":
            return None
        digits = re.sub(r'\D', '', val)
        return digits if digits != "" else None

    df['customer_telnumber'] = df['customer_telnumber'].apply(clean_telnumber)

    # ทำความสะอาดที่อยู่
    df['address'] = df['address'].str.replace('-', '', regex=False)

    def clean_address(val):
        if pd.isnull(val) or val.strip() == "":
            return val
        cleaned = re.sub(r'^[ุูึืิ]+', '', val.strip())
        return cleaned

    df['address'] = df['address'].apply(clean_address)

    def remove_parentheses(val):
        if pd.isnull(val) or val.strip() == "":
            return val
        cleaned = re.sub(r'\([^)]*\)', '', val).strip()
        return cleaned

    df['address'] = df['address'].apply(remove_parentheses)

    def clean_address_final(val):
        if pd.isnull(val) or val.strip() == "":
            return None
        val = val.strip()
        if val == "/":
            return None
        val = val.lstrip(':').strip()
        return val

    df['address'] = df['address'].apply(clean_address_final)

    # แก้ไขข้อมูลเพิ่มเติม
    df['customer_email'] = df['customer_email'].str.replace('_', '', regex=False)
    df['title'] = df['title'].str.replace("'นาย", 'นาย', regex=False).str.strip()

    # ดึงข้อมูลจาก PostgreSQL
    pg_engine = get_postgres_engine()
    query = """
    SELECT *
    FROM dim_customer 
    """
    df5 = pd.read_sql(query, pg_engine)
    df5 = df5.drop(columns=['create_at', 'update_at'])

    # รวมข้อมูล
    df_result = pd.merge(df, df5, on=['customer_card', 'customer_name'], how='right')
    df_result = df_result[['quo_num', 'customer_sk']]
    df_result = df_result.rename(columns={'quo_num': 'quotation_num'})

    # ดึงข้อมูล fact_sales_quotation
    query = """
    SELECT *
    FROM fact_sales_quotation 
    """
    df6 = pd.read_sql(query, pg_engine)
    df6 = df6.drop(columns=['create_at', 'update_at', 'customer_id'])

    df_result1 = pd.merge(df_result, df6, on=['quotation_num'], how='right')
    df_result1 = df_result1.rename(columns={'customer_sk': 'customer_id'})
    df_result1 = df_result1.drop_duplicates(subset=['quotation_num'], keep='last')
    df_result1 = df_result1.where(pd.notnull(df_result1), None)

    # อัปเดตฐานข้อมูล
    update_fact_table(pg_engine, 'fact_sales_quotation', df_result1.to_dict(orient='records'), 'customer_id')
    
    print("✅ Customer merge completed!")

# %%
def merge_order_type_to_quotation():
    """รวมข้อมูล Order Type เข้ากับ Quotation"""
    print("🔄 Starting Order Type merge process...")
    
    engine = get_mariadb_engine()
    task_engine = get_mariadb_task_engine()
    
    # ดึงข้อมูลจาก fin_system_select_plan
    query = """
    SELECT quo_num,type_insure,type_work, type_status , type_key , app_type, chanel_key
    FROM fin_system_select_plan 
    WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
    AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df = pd.read_sql(query, engine)

    # ดึงข้อมูลจาก fin_order
    query = """
    SELECT quo_num,worksend
    FROM fin_order
    """
    df1 = pd.read_sql(query, task_engine)

    # รวมข้อมูล
    df_merged = pd.merge(df, df1, on='quo_num', how='left')

    # แก้ไข chanel_key
    def fill_chanel_key(row):
        chanel_key = row['chanel_key']
        type_key = row['type_key']
        app_type = row['app_type']
        type_insure = row['type_insure']

        if pd.notnull(chanel_key) and str(chanel_key).strip() != "":
            return chanel_key

        if pd.notnull(type_key) and pd.notnull(app_type):
            if type_key == app_type:
                if type_insure == 'ตรอ':
                    return f"{type_key} VIF"
                else:
                    return type_key
            else:
                if type_key in app_type:
                    base = app_type.replace(type_key, "").replace("-", "").strip()
                    return f"{type_key} {base}" if base else type_key
                elif app_type in type_key:
                    base = type_key.replace(app_type, "").replace("-", "").strip()
                    return f"{app_type} {base}" if base else app_type
                else:
                    return f"{type_key} {app_type}"

        if pd.notnull(type_key) and (pd.isnull(app_type) or str(app_type).strip() == ""):
            if pd.notnull(type_insure) and str(type_insure).strip() != "":
                return f"{type_key} {type_insure}"
            else:
                return type_key

        if pd.notnull(app_type) and (pd.isnull(type_key) or str(type_key).strip() == ""):
            if pd.notnull(type_insure) and str(type_insure).strip() != "":
                return f"{app_type} {type_insure}"
            else:
                return app_type

        return None

    df_merged['chanel_key'] = df_merged.apply(fill_chanel_key, axis=1)

    # แก้ไข chanel_key
    df_merged['chanel_key'] = df_merged['chanel_key'].replace({
        'B2B': 'APP B2B',
        'WEB ตรอ': 'WEB VIF',
        'TELE': 'APP TELE',
        'APP-B2C': 'APP B2C',
        'APP ประกันรถ' : 'APP B2B',
        'WEB ประกันรถ': 'WEB'
    })

    df_merged.drop(columns=['type_key', 'app_type'], inplace=True)

    # เปลี่ยนชื่อคอลัมน์
    df_merged.rename(columns={
        "quo_num": "quotation_num",
        "type_insure": "type_insurance",
        "type_work": "order_type",
        "type_status": "check_type",
        "worksend": "work_type",
        "chanel_key": "key_channel"
    }, inplace=True)

    # ทำความสะอาดข้อมูล
    df_merged = df_merged.replace(r'^\s*$', np.nan, regex=True)
    df_merged = df_merged.where(pd.notnull(df_merged), None)
    df_merged = df_merged.replace("NaN", np.nan)
    df_merged = df_merged.drop_duplicates(subset=['quotation_num'], keep='first')

    # ดึงข้อมูลจาก PostgreSQL
    pg_engine = get_postgres_engine()
    query = """
    SELECT *
    FROM dim_order_type 
    """
    df5 = pd.read_sql(query, pg_engine)
    df5 = df5.drop(columns=['create_at', 'update_at'])

    # รวมข้อมูล
    df_result = pd.merge(df_merged, df5, on=['quotation_num'], how='right')
    df_result = df_result[['quotation_num', 'order_type_id']]

    # ดึงข้อมูล fact_sales_quotation
    query = """
    SELECT *
    FROM fact_sales_quotation 
    """
    df6 = pd.read_sql(query, pg_engine)
    df6 = df6.drop(columns=['create_at', 'update_at', 'order_type_id'])

    df_result1 = pd.merge(df_result, df6, on=['quotation_num'], how='right')
    df_result1 = df_result1.drop_duplicates(subset=['quotation_num'], keep='last')
    df_result1 = df_result1.where(pd.notnull(df_result1), None)

    # อัปเดตฐานข้อมูล
    update_fact_table(pg_engine, 'fact_sales_quotation', df_result1.to_dict(orient='records'), 'order_type_id')

    # ลบคอลัมน์ quotation_num ในตาราง dim_order_type
    with pg_engine.begin() as conn:
        conn.execute(text("ALTER TABLE dim_order_type DROP COLUMN quotation_num;"))

    print("✅ Order Type merge completed!")

# %%
def merge_payment_plan_to_quotation():
    """รวมข้อมูล Payment Plan เข้ากับ Quotation"""
    print("🔄 Starting Payment Plan merge process...")
    
    engine = get_mariadb_engine()
    task_engine = get_mariadb_task_engine()
    
    # ดึงข้อมูลจาก fin_system_select_plan
    query = """
    SELECT quo_num, type_insure
    FROM fin_system_select_plan
    WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
    AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df = pd.read_sql(query, engine)

    # ดึงข้อมูลจาก fin_system_pay
    query = """
    SELECT quo_num, chanel_main, clickbank, chanel, numpay, condition_install
    FROM fin_system_pay
    WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
    AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df2 = pd.read_sql(query, engine)

    # แก้ไข chanel
    df2['chanel'] = df2['chanel'].replace({
        'ผ่อนบัตร': 'เข้าฟิน'
    })

    # ดึงข้อมูลจาก fin_order
    query = """
    SELECT quo_num, status_paybill
    FROM fininsurance_task.fin_order
    WHERE type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df4 = pd.read_sql(query, task_engine)

    # รวมข้อมูล
    df_merged1 = pd.merge(df2, df4, on=['quo_num'], how='left')

    # เปลี่ยนชื่อคอลัมน์
    df_merged1 = df_merged1.rename(columns={
        'quo_num': 'quotation_num',
        'type_insure': 'type_insurance',
        'chanel': 'payment_reciever',
        'status_paybill': 'payment_type',
    })

    # กำหนด payment_channel
    def determine_payment_channel(row):
        ch_main = str(row['chanel_main']).strip().lower()
        cb_raw = row['clickbank']
        cb = str(cb_raw).strip().lower()
        is_cb_empty = pd.isna(cb_raw) or cb == ''

        if ch_main in ['ตัดบัตรเครดิต', 'ผ่อนบัตร', 'ผ่อนบัตรเครดิต', 'ผ่อนชำระ']:
            if 'qrcode' in cb:
                return 'QR Code'
            elif 'creditcard' in cb:
                return '2C2P'
            else:
                return 'ตัดบัตรกับฟิน'

        if ch_main in ['โอนเงิน', 'ผ่อนโอน']:
            if 'qrcode' in cb:
                return 'QR Code'
            else:
                return 'โอนเงิน'

        if ch_main and is_cb_empty:
            return row['chanel_main']
        elif not ch_main and not is_cb_empty:
            if 'qrcode' in cb:
                return 'QR Code'
            elif 'creditcard' in cb:
                return '2C2P'
            else:
                return row['clickbank']
        elif not is_cb_empty:
            if 'qrcode' in cb:
                return 'QR Code'
            elif 'creditcard' in cb:
                return '2C2P'
            else:
                return row['clickbank']
        else:
            return ''

    df_merged1['payment_channel'] = df_merged1.apply(determine_payment_channel, axis=1)

    # ลบคอลัมน์ที่ไม่ต้องการ
    df_merged1.drop(columns=['chanel_main', 'clickbank', 'condition_install'], inplace=True)
    df_merged1 = df_merged1.rename(columns={'numpay': 'installment_number'})

    # ลบข้อมูลที่ลงท้ายด้วย -r
    df_merged1 = df_merged1[~df_merged1['quotation_num'].str.endswith('-r', na=False)]

    # แก้ไขข้อมูล
    df_merged1 = df_merged1.replace(['', np.nan], None)
    df_merged1['installment_number'] = df_merged1['installment_number'].replace({0: 1})

    # ลบข้อมูลทดสอบ
    query = """
    SELECT * 
    FROM fininsurance.fin_system_select_plan
    where name in ('ทดสอบ','test')
    and datestart >= '2025-05-01' AND datestart < '2025-07-01'
    AND type_insure IN ('ประกันรถ', 'ตรอ');
    """
    dele = pd.read_sql(query, engine)
    dele = dele.rename(columns={'quo_num': 'quotation_num', 'num_pay': 'installment_number'})
    df_merged1 = df_merged1[~df_merged1['quotation_num'].isin(dele['quotation_num'])]
    df_merged1 = df_merged1[df_merged1['quotation_num'] != 'FQ2505-24999']

    # ดึงข้อมูลจาก PostgreSQL
    pg_engine = get_postgres_engine()
    query = """
    SELECT *
    FROM dim_payment_plan 
    """
    df5 = pd.read_sql(query, pg_engine)
    df5 = df5.drop(columns=['create_at', 'update_at'])

    # รวมข้อมูล
    df_result = pd.merge(df_merged1, df5, on=['quotation_num'], how='right')
    df_result = df_result[['quotation_num', 'payment_plan_id']]

    # ดึงข้อมูล fact_sales_quotation
    query = """
    SELECT *
    FROM fact_sales_quotation 
    """
    df6 = pd.read_sql(query, pg_engine)
    df6 = df6.drop(columns=['create_at', 'update_at', 'payment_plan_id'])

    df_result1 = pd.merge(df_result, df6, on=['quotation_num'], how='right')
    df_result1 = df_result1.drop_duplicates(subset=['quotation_num'], keep='last')
    df_result1 = df_result1.where(pd.notnull(df_result1), None)

    # อัปเดตฐานข้อมูล
    update_fact_table(pg_engine, 'fact_sales_quotation', df_result1.to_dict(orient='records'), 'payment_plan_id')

    # ลบคอลัมน์ quotation_num ในตาราง dim_payment_plan
    with pg_engine.begin() as conn:
        conn.execute(text("ALTER TABLE dim_payment_plan DROP COLUMN quotation_num;"))

    print("✅ Payment Plan merge completed!")

# %%
def main():
    """ฟังก์ชันหลักสำหรับรันการรวมข้อมูลทั้งหมด"""
    print("🚀 Starting all dimension merge processes...")
    
    try:
        # รวมข้อมูล Agent
        merge_agent_to_quotation()
        
        # รวมข้อมูล Car
        merge_car_to_quotation()
        
        # รวมข้อมูล Customer
        merge_customer_to_quotation()
        
        # รวมข้อมูล Order Type
        merge_order_type_to_quotation()
        
        # รวมข้อมูล Payment Plan
        merge_payment_plan_to_quotation()
        
        print("🎉 All dimension merge processes completed successfully!")
        
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        raise

# %%
if __name__ == "_