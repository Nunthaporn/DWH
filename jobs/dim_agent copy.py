from dagster import op, job
import pandas as pd
import numpy as np
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta

# ✅ โหลด .env
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
    f"postgresql+psycopg2://{target_user}:{target_password}@{target_host}:{target_port}/{target_db}",
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c statement_timeout=300000"  # 5 นาที
    },
    pool_pre_ping=True
)

@op
def extract_agent_data():

    query_main = text("""
    SELECT cuscode, name, rank,
           user_registered,
           status, fin_new_group, fin_new_mem,
           type_agent, typebuy, user_email, name_store, address, city, district,
           province, province_cur, area_cur, postcode, tel, date_active, card_ins_type, file_card_ins,
           card_ins_type_life, file_card_ins_life
    FROM wp_users
    WHERE user_login NOT IN ('FINTEST-01', 'FIN-TestApp', 'Admin-VIF', 'adminmag_fin', 'FNG00-00001')
        AND name NOT LIKE '%%ทดสอบ%%'
        AND name NOT LIKE '%%tes%%'
        AND cuscode NOT LIKE 'ADMIN-SALE001%%'
        AND cuscode NOT LIKE 'center_sale%%'
        AND cuscode NOT LIKE 'Client-sale%%'
        AND cuscode NOT LIKE 'Sale%%'
        AND cuscode NOT LIKE 'south%%'
        AND cuscode NOT LIKE 'mng_sale%%'
        AND cuscode NOT LIKE 'bkk%%'
        AND cuscode NOT LIKE 'east%%'
        AND cuscode NOT LIKE 'north%%'
        AND cuscode NOT LIKE 'central%%'
        AND cuscode NOT LIKE 'upc%%'
        AND cuscode NOT LIKE 'sqc_%%'
        AND cuscode NOT LIKE 'pm_%%'
        AND cuscode NOT LIKE 'Sale-Tor%%'
        AND cuscode NOT LIKE 'online%%'
        AND cuscode NOT LIKE 'Sale-Direct%%'
        AND name NOT LIKE '%%ทดสอบ%%'
        AND name NOT LIKE '%%test%%'
        AND name NOT LIKE '%%เทสระบบ%%'
        AND name NOT LIKE '%%Tes ระบบ%%'
        AND name NOT LIKE '%%ทด่ท%%'
        AND name NOT LIKE '%%ทด สอบ%%'
        AND name NOT LIKE '%%ปัญญวัฒน์ โพธิ์ศรีทอง%%'
        AND name NOT LIKE '%%เอกศิษฎ์ เจริญธันยบูรณ์%%'
        AND cuscode NOT LIKE '%%FIN-TestApp%%'
        AND cuscode NOT LIKE '%%FIN-Tester1%%'
        AND cuscode NOT LIKE '%%FIN-Tester2%%';
    """)

    df_main = pd.read_sql(query_main, source_engine)

    query_career = "SELECT cuscode, career FROM policy_register"
    df_career = pd.read_sql(query_career, source_engine)

    # ✅ Debug: ตรวจสอบข้อมูลในตาราง policy_register
    print(f"🔍 Policy register analysis:")
    print(f"   - Total records in policy_register: {len(df_career)}")
    print(f"   - Unique cuscode in policy_register: {df_career['cuscode'].nunique()}")
    print(f"   - Career values: {df_career['career'].value_counts().head(10)}")
    
    # ✅ ตรวจสอบ cuscode ที่ซ้ำกันใน policy_register
    duplicate_cuscode = df_career[df_career['cuscode'].duplicated(keep=False)]
    if len(duplicate_cuscode) > 0:
        print(f"   - Duplicate cuscode in policy_register: {len(duplicate_cuscode)}")
        print(f"   - Sample duplicates:")
        print(duplicate_cuscode.head(5))
    
    # ✅ ตรวจสอบความสัมพันธ์ระหว่าง cuscode
    main_cuscode_set = set(df_main['cuscode'])
    career_cuscode_set = set(df_career['cuscode'])
    
    print(f"🔍 Cuscode relationship analysis:")
    print(f"   - Unique cuscode in wp_users: {len(main_cuscode_set)}")
    print(f"   - Unique cuscode in policy_register: {len(career_cuscode_set)}")
    print(f"   - Cuscode in both tables: {len(main_cuscode_set & career_cuscode_set)}")
    print(f"   - Cuscode only in wp_users: {len(main_cuscode_set - career_cuscode_set)}")
    print(f"   - Cuscode only in policy_register: {len(career_cuscode_set - main_cuscode_set)}")
    
    # แสดงตัวอย่าง cuscode ที่มีเฉพาะใน wp_users
    only_in_main = main_cuscode_set - career_cuscode_set
    if len(only_in_main) > 0:
        sample_only_main = df_main[df_main['cuscode'].isin(list(only_in_main)[:5])][['cuscode', 'name']]
        print(f"🔍 Sample cuscode only in wp_users:")
        print(sample_only_main)

    df_merged = pd.merge(df_main, df_career, on='cuscode', how='left')

    print("📦 df_main:", df_main.shape)
    print("📦 df_career:", df_career.shape)
    print("📦 df_merged:", df_merged.shape)
    
    # ✅ Debug: ตรวจสอบข้อมูล career
    career_null_count = df_merged['career'].isna().sum()
    career_total_count = len(df_merged)
    print(f"🔍 Career data analysis:")
    print(f"   - Total records: {career_total_count}")
    print(f"   - Records with career: {career_total_count - career_null_count}")
    print(f"   - Records without career (NaN): {career_null_count}")
    print(f"   - Percentage with career: {((career_total_count - career_null_count) / career_total_count * 100):.2f}%")
    
    # ✅ แสดงตัวอย่างข้อมูลที่ไม่มี career
    if career_null_count > 0:
        sample_null_career = df_merged[df_merged['career'].isna()][['cuscode', 'name']].head(5)
        print(f"🔍 Sample records without career:")
        print(sample_null_career)
    
    # ✅ แสดงตัวอย่างข้อมูลที่มี career
    career_not_null = df_merged[df_merged['career'].notna()]
    if len(career_not_null) > 0:
        sample_with_career = career_not_null[['cuscode', 'name', 'career']].head(5)
        print(f"🔍 Sample records with career:")
        print(sample_with_career)
    
    # ✅ ทำความสะอาดข้อมูล career
    print(f"🔍 Cleaning career data...")
    
    # แปลง career เป็น string และทำความสะอาด
    df_merged['career'] = df_merged['career'].astype(str)
    df_merged['career'] = df_merged['career'].str.strip()
    
    # แสดงผลลัพธ์หลังทำความสะอาด
    career_after_clean = df_merged['career'].value_counts()
    print(f"🔍 Career values after cleaning:")
    print(career_after_clean.head(10))

    return df_merged


@op
def clean_agent_data(df: pd.DataFrame):
    # Combine region columns
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

    df['agent_region'] = df.apply(lambda row: combine_columns(row['fin_new_group'], row['fin_new_mem']), axis=1)
    # ✅ กรอง row ที่ agent_region = 'TEST' ออก
    df = df[df['agent_region'] != 'TEST']
    df = df.drop(columns=['fin_new_group', 'fin_new_mem'])

    # Clean date_active and status_agent
    df['date_active'] = pd.to_datetime(df['date_active'], errors='coerce')
    now = pd.Timestamp.now()
    one_month_ago = now - pd.DateOffset(months=1)

    def check_condition(row):
        if row['status'] == 'defect':
            return 'inactive'
        elif pd.notnull(row['date_active']) and row['date_active'] < one_month_ago:
            return 'inactive'
        else:
            return 'active'

    df['status_agent'] = df.apply(check_condition, axis=1)
    df = df.drop(columns=['status', 'date_active'])

    df['defect_status'] = np.where(df['cuscode'].str.contains('-defect', na=False), 'defect', None)

    # Rename columns
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
        "agent_region": "agent_region",
        "defect_status": "defect_status",
        "card_ins_type": "card_ins_type",
        "file_card_ins": "file_card_ins",
        "card_ins_type_life": "card_ins_type_life",
        "file_card_ins_life": "file_card_ins_life"
    }
    df = df.rename(columns=rename_columns)
    df['defect_status'] = np.where(df['agent_id'].str.contains('-defect', na=False), 'defect', None)

    # Clean fields
    df['card_ins_type_life'] = df['card_ins_type_life'].apply(lambda x: 'B' if isinstance(x, str) and 'แทน' in x else x)
    df['is_experienced_fix'] = df['is_experienced'].apply(lambda x: 'เคยขาย' if str(x).strip().lower() == 'ไม่เคยขาย' else 'ไม่เคยขาย')
    df = df.drop(columns=['is_experienced'])
    df.rename(columns={'is_experienced_fix': 'is_experienced'}, inplace=True)

    valid_rank = [str(i) for i in range(1, 11)]
    df.loc[~df['agent_rank'].isin(valid_rank), 'agent_rank'] = np.nan

    df['agent_address_cleaned'] = df['agent_address'].apply(lambda addr: re.sub(r'(เลขที่|หมู่ที่|หมู่บ้าน|ซอย|ถนน)[\s\-]*', '', str(addr)).strip())
    df = df.drop(columns=['agent_address'])
    df.rename(columns={'agent_address_cleaned': 'agent_address'}, inplace=True)

    df['mobile_number'] = df['mobile_number'].str.replace(r'[^0-9]', '', regex=True)
    df = df.replace(r'^\s*$', pd.NA, regex=True)

    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df['non_empty_count'] = df_temp.notnull().sum(axis=1)

    valid_mask = df['agent_id'].astype(str).str.strip().ne('') & df['agent_id'].notna()
    df_with_id = df[valid_mask]
    df_without_id = df[~valid_mask]
    df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='agent_id', keep='first')
    df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)
    df_cleaned = df_cleaned.drop(columns=['non_empty_count'])
    df_cleaned.columns = df_cleaned.columns.str.lower()

    df_cleaned["hire_date"] = pd.to_datetime(df_cleaned["hire_date"], errors='coerce')
    df_cleaned["hire_date"] = df_cleaned["hire_date"].dt.strftime('%Y%m%d')
    df_cleaned["hire_date"] = df_cleaned["hire_date"].where(df_cleaned["hire_date"].notnull(), None)
    df_cleaned["hire_date"] = df_cleaned["hire_date"].astype('Int64')

    df_cleaned["zipcode"] = df_cleaned["zipcode"].where(df_cleaned["zipcode"].str.len() == 5, np.nan)
    df_cleaned["agent_name"] = df_cleaned["agent_name"].str.lstrip()
    df_cleaned["is_experienced"] = df_cleaned["is_experienced"].apply(lambda x: 'no' if str(x).strip().lower() == 'ไม่เคยขาย' else 'yes')

    # ✅ ทำความสะอาด job/career column - แปลง NaN และ None เป็น None
    df_cleaned["job"] = df_cleaned["job"].where(pd.notna(df_cleaned["job"]), None)
    df_cleaned["job"] = df_cleaned["job"].replace('NaN', None)
    df_cleaned["job"] = df_cleaned["job"].replace('nan', None)
    df_cleaned["job"] = df_cleaned["job"].replace('NULL', None)
    df_cleaned["job"] = df_cleaned["job"].replace('null', None)

    # ✅ เปลี่ยน "Others" เป็น "อื่นๆ" ในคอลัมน์ province
    df_cleaned["province"] = df_cleaned["province"].replace("Others", "อื่นๆ")

    # ✅ ทำความสะอาด agent_email - เก็บเฉพาะภาษาอังกฤษและรูปแบบ email ที่ถูกต้อง
    def clean_email(email):
        if pd.isna(email) or email == '':
            return None
        
        email_str = str(email).strip()
        
        # ตรวจสอบว่ามีตัวอักษรไทยหรือไม่
        thai_chars = re.findall(r'[ก-๙]', email_str)
        if thai_chars:
            return None
        
        # ตรวจสอบรูปแบบ email ที่ถูกต้อง
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.match(email_pattern, email_str):
            return email_str.lower()  # แปลงเป็นตัวพิมพ์เล็ก
        else:
            return None
    
    df_cleaned["agent_email"] = df_cleaned["agent_email"].apply(clean_email)

    # ✅ ทำความสะอาด agent_name - ลบสระพิเศษด้านหน้าแต่ไม่ลบสระในชื่อ
    def clean_agent_name(name):
        if pd.isna(name) or name == '':
            return None
        
        name_str = str(name).strip()
        
        # ลบเฉพาะสระพิเศษที่อยู่ด้านหน้าชื่อ (ไม่ลบสระในชื่อจริง)
        # ตรวจสอบว่าสระด้านหน้าเป็นสระพิเศษหรือไม่
        if name_str.startswith(('ิ', 'ี', 'ึ', 'ื', 'ุ', 'ู', '่', '้', '๊', '๋')): 
            # ลบเฉพาะสระพิเศษที่อยู่ด้านหน้าเท่านั้น
            cleaned_name = re.sub(r'^[ิีึืุู่้๊๋]+', '', name_str)
        else:
            cleaned_name = name_str
        
        # ลบตัวอักษรพิเศษที่ไม่ควรมีในชื่อ (ยกเว้นตัวอักษรไทยและอังกฤษ)
        cleaned_name = re.sub(r'[^\u0E00-\u0E7F\u0020\u0041-\u005A\u0061-\u007A]', '', cleaned_name)
        
        # ลบช่องว่างที่ซ้ำกัน
        cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
        
        # ลบช่องว่างที่ต้นและท้าย
        cleaned_name = cleaned_name.strip()
        
        # ตรวจสอบว่าชื่อมีความยาวที่เหมาะสม (อย่างน้อย 2 ตัวอักษร)
        if len(cleaned_name) < 2:
            return None
        
        return cleaned_name

    df_cleaned["agent_name"] = df_cleaned["agent_name"].apply(clean_agent_name)

    # ✅ ทำความสะอาด store_name - เก็บเฉพาะชื่อร้าน
    def clean_store_name(store_name):
        if pd.isna(store_name) or store_name == '':
            return None
        
        store_str = str(store_name).strip()
        
        # ถ้ามีคำว่า "ร้าน" หรือ "shop" หรือ "store" ให้เก็บไว้
        if any(keyword in store_str.lower() for keyword in ['ร้าน', 'shop', 'store', 'บริษัท', 'company']):
            return store_str
        else:
            return None
    
    df_cleaned["store_name"] = df_cleaned["store_name"].apply(clean_store_name)

    # ✅ ตรวจสอบและเปลี่ยนค่าที่ไม่ใช่ภาษาไทยเป็น None ในคอลัมน์ที่ระบุ
    def check_thai_text(text):
        if pd.isna(text) or text == '':
            return None
        
        text_str = str(text).strip()
        
        # ลบ space และ ] หรือ * ที่อยู่ด้านหน้าข้อความ
        cleaned_text = re.sub(r'^[\s\]\*]+', '', text_str)
        
        # กรองข้อมูลที่ไม่ต้องการ
        unwanted_patterns = [
            r'^\d+$',    # เป็นตัวเลขล้วนๆ
            r'^[A-Za-z\s]+$',  # เป็นภาษาอังกฤษล้วนๆ
        ]
        
        for pattern in unwanted_patterns:
            if re.match(pattern, cleaned_text):
                return None
        
        # ตรวจสอบว่ามีตัวอักษรไทยหรือไม่
        thai_chars = re.findall(r'[ก-๙]', cleaned_text)
        if thai_chars:
            return cleaned_text
        else:
            return None
    
    # ใช้ฟังก์ชันกับคอลัมน์ที่ระบุ
    location_columns = ['subdistrict', 'district', 'province', 'current_province', 'current_area']
    for col in location_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(check_thai_text)

    # ✅ ทำความสะอาด zipcode - เก็บเฉพาะตัวเลข 5 หลัก
    def clean_zipcode(zipcode):
        if pd.isna(zipcode) or zipcode == '':
            return None
        
        zipcode_str = str(zipcode).strip()
        
        # ตรวจสอบว่าเป็นตัวเลข 5 หลักหรือไม่
        if re.match(r'^\d{5}$', zipcode_str):
            return zipcode_str
        else:
            return None
    
    df_cleaned["zipcode"] = df_cleaned["zipcode"].apply(clean_zipcode)

    # ✅ กรองแถวที่มี card_ins_type เป็น "ญาตเป็นนายหน้า"
    df_cleaned = df_cleaned[df_cleaned["card_ins_type"] != "ญาตเป็นนายหน้า"]

    # ✅ ทำความสะอาด agent_address - ลบสระด้านหน้า, -, :, . และตัวอักษรพิเศษ
    def clean_address(address):
        if pd.isna(address) or address == '':
            return None
        
        address_str = str(address).strip()
        
        # ลบสระพิเศษที่อยู่ด้านหน้าข้อความ (สระที่ไม่ได้เป็นส่วนของที่อยู่จริง)
        # ลบเครื่องหมาย -, :, . และตัวอักษรพิเศษ
        # ใช้ regex ที่ครอบคลุมสระทั้งหมดที่อยู่ด้านหน้า
        cleaned_address = re.sub(r'^[\u0E30-\u0E3A\u0E47-\u0E4E]+', '', address_str)  # ลบสระด้านหน้า
        cleaned_address = re.sub(r'[-:.,]', '', cleaned_address)  # ลบเครื่องหมายพิเศษ
        
        # ลบช่องว่างที่ซ้ำกัน
        cleaned_address = re.sub(r'\s+', ' ', cleaned_address)
        
        # ลบช่องว่างที่ต้นและท้าย
        cleaned_address = cleaned_address.strip()
        
        return cleaned_address
    
    df_cleaned["agent_address"] = df_cleaned["agent_address"].apply(clean_address)

    # ✅ ทำความสะอาด agent_address column - แปลง NaN และ None เป็น None
    df_cleaned["agent_address"] = df_cleaned["agent_address"].where(pd.notna(df_cleaned["agent_address"]), None)
    df_cleaned["agent_address"] = df_cleaned["agent_address"].replace('NaN', None)
    df_cleaned["agent_address"] = df_cleaned["agent_address"].replace('None', None)
    df_cleaned["agent_address"] = df_cleaned["agent_address"].replace('NULL', None)

    # ✅ ลบ space ที่อยู่ด้านหน้าข้อความในทุกคอลัมน์
    def clean_leading_spaces(text):
        if pd.isna(text) or text == '':
            return None
        
        text_str = str(text).strip()
        # ลบ space ที่อยู่ด้านหน้าข้อความ
        cleaned_text = re.sub(r'^\s+', '', text_str)
        
        return cleaned_text if cleaned_text != '' else None
    
    # ใช้ฟังก์ชันกับทุกคอลัมน์
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':  # เฉพาะคอลัมน์ที่เป็นข้อความ
            df_cleaned[col] = df_cleaned[col].apply(clean_leading_spaces)

    print("\n📊 Cleaning completed")

    return df_cleaned


@op
def load_to_wh(df: pd.DataFrame):
    table_name = 'dim_agent'
    pk_column = 'agent_id'

    df = df[~df[pk_column].duplicated(keep='first')].copy()

    # ✅ วันปัจจุบัน (เริ่มต้นเวลา 00:00:00)
    today_str = datetime.now().strftime('%Y-%m-%d')

    # ✅ Load เฉพาะข้อมูลวันนี้จาก PostgreSQL
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(
            f"SELECT * FROM {table_name} WHERE update_at >= '{today_str}'",
            conn
        )

    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    # ✅ Debug: แสดงจำนวนข้อมูลในแต่ละส่วน
    print(f"🔍 Total new data: {len(df)}")
    print(f"🔍 Existing data today: {len(df_existing)}")
    print(f"🔍 New IDs to insert: {len(new_ids)}")
    print(f"🔍 Common IDs to compare: {len(common_ids)}")
    print(f"🔍 Data to insert: {len(df_to_insert)}")
    print(f"🔍 Common new data: {len(df_common_new)}")
    print(f"🔍 Common old data: {len(df_common_old)}")

    # ✅ ตรวจสอบว่ามีข้อมูลที่ซ้ำกันหรือไม่
    if df_common_new.empty or df_common_old.empty:
        print("ℹ️ No common data to compare, skipping update logic")
        merged = pd.DataFrame()
    else:
        merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

    # ✅ ตรวจสอบว่า merged DataFrame ว่างเปล่าหรือไม่
    if merged.empty:
        print("ℹ️ No merged data, skipping comparison")
        df_diff = pd.DataFrame()
        df_diff_renamed = pd.DataFrame()
    else:
        # ✅ Debug: แสดงคอลัมน์ที่มีอยู่ใน merged DataFrame
        print(f"🔍 Merged columns: {list(merged.columns)}")
        print(f"🔍 New data columns: {list(df.columns)}")

    exclude_columns = [pk_column, 'id_contact', 'create_at', 'update_at']
    compare_cols = [
        col for col in df.columns
        if col not in exclude_columns
        and f"{col}_new" in merged.columns
        and f"{col}_old" in merged.columns
    ]

    # ✅ Debug: แสดงคอลัมน์ที่จะเปรียบเทียบ
    print(f"🔍 Compare columns: {compare_cols}")
        
    # ✅ Debug: แสดงคอลัมน์ที่มีอยู่ใน df และ merged
    print(f"🔍 df columns: {list(df.columns)}")
    print(f"🔍 merged columns with _new suffix: {[col for col in merged.columns if col.endswith('_new')]}") 
    print(f"🔍 merged columns with _old suffix: {[col for col in merged.columns if col.endswith('_old')]}")

    # ✅ ตรวจสอบว่ามีคอลัมน์ที่จะเปรียบเทียบหรือไม่
    if not compare_cols:
        print("⚠️ No columns to compare, skipping update")
        df_diff_renamed = pd.DataFrame()
    else:
        def is_different(row):
            for col in compare_cols:
                val_new = row.get(f"{col}_new")
                val_old = row.get(f"{col}_old")
                if pd.isna(val_new) and pd.isna(val_old):
                    continue
                elif pd.isna(val_new) or pd.isna(val_old):
                    return True
                elif val_new != val_old:
                    return True
            return False

        df_diff = merged[merged.apply(is_different, axis=1)].copy()
            
        # ✅ ตรวจสอบว่ามีข้อมูลที่แตกต่างหรือไม่
        if df_diff.empty:
            print("ℹ️ No differences found, skipping update")
            df_diff_renamed = pd.DataFrame()
        else:
            update_cols = [f"{col}_new" for col in compare_cols]
            all_cols = [pk_column] + update_cols
                
            # ✅ ตรวจสอบว่าคอลัมน์ทั้งหมดมีอยู่ใน df_diff หรือไม่
            missing_cols = [col for col in all_cols if col not in df_diff.columns]
            if missing_cols:
                print(f"⚠️ Missing columns in df_diff: {missing_cols}")
                # ใช้เฉพาะคอลัมน์ที่มีอยู่
                available_cols = [col for col in all_cols if col in df_diff.columns]
                df_diff_renamed = df_diff[available_cols].copy()
                # แปลงชื่อคอลัมน์กลับ
                available_compare_cols = [col.replace('_new', '') for col in available_cols if col != pk_column]
                df_diff_renamed.columns = [pk_column] + available_compare_cols
            else:
                df_diff_renamed = df_diff[all_cols].copy()
            df_diff_renamed.columns = [pk_column] + compare_cols

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

    metadata_table = Table(table_name, MetaData(), autoload_with=target_engine)

    def chunk_dataframe(df, chunk_size=500):
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i + chunk_size]

    # ✅ Insert batch (ใช้ UPSERT แทน INSERT)
    if not df_to_insert.empty:
        # ✅ ลบข้อมูลซ้ำใน df_to_insert
        df_to_insert = df_to_insert[~df_to_insert[pk_column].duplicated(keep='first')].copy()
        print(f"🔍 After removing duplicates in insert data: {len(df_to_insert)}")
        
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped} rows")
        if not df_to_insert_valid.empty:
            with target_engine.begin() as conn:
                for batch_df in chunk_dataframe(df_to_insert_valid):
                    stmt = pg_insert(metadata_table).values(batch_df.to_dict(orient="records"))
                    valid_column_names = [c.name for c in metadata_table.columns]
                    update_columns = {
                        c: stmt.excluded[c]
                        for c in valid_column_names
                        if c != pk_column and c in batch_df.columns
                    }
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[pk_column],
                        set_=update_columns
                    )
                    conn.execute(stmt)

    # ✅ Update batch
    if not df_diff_renamed.empty:
        with target_engine.begin() as conn:
            for batch_df in chunk_dataframe(df_diff_renamed):
                stmt = pg_insert(metadata_table).values(batch_df.to_dict(orient="records"))
                valid_column_names = [c.name for c in metadata_table.columns]
                update_columns = {
                    c: stmt.excluded[c]
                    for c in valid_column_names
                    if c not in [pk_column, 'id_contact', 'create_at', 'update_at'] and c in batch_df.columns
                }
                # update_at ให้เป็นเวลาปัจจุบัน
                update_columns['update_at'] = datetime.now()
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")

@op
def clean_null_values_op(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(['None', 'none', 'nan', 'NaN', ''], np.nan)

@job
def dim_agent_etl():
    load_to_wh(clean_agent_data(clean_null_values_op(extract_agent_data())))

if __name__ == "__main__":
    df_raw = extract_agent_data()
    print("✅ Extracted logs:", df_raw.shape)

    df_clean = clean_agent_data((df_raw))
    print("✅ Cleaned columns:", df_clean.columns)

    df_clean = clean_null_values_op(df_clean)

    # output_path = "dim_agent.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_to_wh(df_clean)
    print("🎉 Test completed! Data upserted to dim_agent.")
