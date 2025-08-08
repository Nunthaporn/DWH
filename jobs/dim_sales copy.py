from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta

# ✅ โหลด env
load_dotenv()

# ✅ DB Connections
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)

target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_sales_data():
    # now = datetime.now()

    # start_time = now.replace(minute=0, second=0, microsecond=0)
    # end_time = now.replace(minute=59, second=59, microsecond=999999)

    # start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    # end_str = end_time.strftime('%Y-%m-%d %H:%M:%S') 

    query_main = text("""
        SELECT cuscode, name, rank, user_registered,
            status, fin_new_group, fin_new_mem,
            type_agent, typebuy, user_email, name_store, address, city, district,
            province, province_cur, area_cur, postcode, tel, date_active,
            card_ins_type, file_card_ins, card_ins_type_life, file_card_ins_life
        FROM wp_users
        WHERE user_login NOT IN ('FINTEST-01', 'FIN-TestApp', 'Admin-VIF', 'adminmag_fin', 'FNG00-00001')
        AND name NOT LIKE '%ทดสอบ%'
        AND name NOT LIKE '%tes%'
        AND cuscode NOT LIKE '%FNG%'
        AND cuscode NOT LIKE '%FTR%'
        AND cuscode NOT LIKE '%FIN%'
        AND cuscode NOT LIKE '%FIT%'
        AND name NOT LIKE '%ทดสอบ%'
        AND name NOT LIKE '%test%'
        AND name NOT LIKE '%เทสระบบ%'
        AND name NOT LIKE '%Tes ระบบ%'
        AND name NOT LIKE '%ทด่ท%'
        AND name NOT LIKE '%ทด สอบ%'
        AND name NOT LIKE '%ปัญญวัฒน์ โพธิ์ศรีทอง%'
        AND name NOT LIKE '%เอกศิษฎ์ เจริญธันยบูรณ์%'
        AND cuscode NOT LIKE '%%@gmail%%'
        AND cuscode NOT LIKE '%%@mail%%'
        AND cuscode NOT LIKE '%%@hotmail%%'
        AND cuscode NOT LIKE '%%@icloud%%'
        AND cuscode NOT LIKE '%%@windowslive%%'
        AND cuscode NOT LIKE '%%@outlook%%'
        AND cuscode NOT LIKE '%%@lcloud%%'
        AND cuscode NOT LIKE '%%@outloot%%'
        AND cuscode NOT LIKE '%%@yahoo%%'
        AND cuscode NOT LIKE '%%TROAgency%%'
        AND cuscode NOT LIKE '%%Direct%%'
        AND cuscode NOT LIKE '%%direct%%'
        AND cuscode NOT LIKE '%%Client%%'
        AND cuscode NOT LIKE '%%Account%%'
        AND cuscode NOT LIKE '%%ADMIN%%'
        AND cuscode NOT LIKE '%%WEB-B2C%%'
        AND cuscode NOT LIKE '%%WEB-B2CQQ%%'
        AND cuscode NOT LIKE '%%Website%%'
        AND cuscode NOT LIKE '%%CUS%%'
        AND cuscode NOT LIKE '%%REGISTER%%'
        AND cuscode NOT LIKE '%FIN-TestApp%'
        AND cuscode NOT LIKE '%FIN-Tester1%'
        AND cuscode NOT LIKE '%FIN-Tester2%';
    """)
                      
    df_main = pd.read_sql(query_main, source_engine)
    # แปลง NaN string เป็น None
    df_main = df_main.replace('NaN', None)
    df_main = df_main.replace('nan', None)
    df_main = df_main.replace('NULL', None)
    df_main = df_main.replace('null', None)
    
    # แปลง pandas NaN เป็น None
    df_main = df_main.where(pd.notna(df_main), None)
    
    query_career = text("SELECT cuscode, career FROM policy_register")
    df_career = pd.read_sql(query_career, source_engine)
    
    # แปลง NaN string เป็น None ใน career data
    df_career = df_career.replace('NaN', None)
    df_career = df_career.replace('nan', None)
    df_career = df_career.replace('NULL', None)
    df_career = df_career.replace('null', None)
    
    # แปลง pandas NaN เป็น None
    df_career = df_career.where(pd.notna(df_career), None)

    return pd.merge(df_main, df_career, on='cuscode', how='left')

@op
def clean_sales_data(df: pd.DataFrame):
    # แปลง NaN string เป็น None ในข้อมูลที่เข้ามา
    df = df.replace('NaN', None)
    df = df.replace('nan', None)
    df = df.replace('NULL', None)
    df = df.replace('null', None)
    
    # แปลง pandas NaN เป็น None
    df = df.where(pd.notna(df), None)
    
    # Combine region columns
    def combine_columns(a, b):
        a_str = str(a).strip() if a is not None else ''
        b_str = str(b).strip() if b is not None else ''
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
        elif pd.notna(row['date_active']) and row['date_active'] < one_month_ago:
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
    df.loc[~df['agent_rank'].isin(valid_rank), 'agent_rank'] = None

    df['agent_address_cleaned'] = df['agent_address'].apply(lambda addr: re.sub(r'(เลขที่|หมู่ที่|หมู่บ้าน|ซอย|ถนน)[\s\-]*', '', str(addr)).strip())
    df = df.drop(columns=['agent_address'])
    df.rename(columns={'agent_address_cleaned': 'agent_address'}, inplace=True)

    df['mobile_number'] = df['mobile_number'].str.replace(r'[^0-9]', '', regex=True)
    # แปลง empty string เป็น None
    df = df.replace(r'^\s*$', None, regex=True)

    # นับจำนวนคอลัมน์ที่ไม่ใช่ None
    df['non_empty_count'] = df.notna().sum(axis=1)

    valid_mask = df['agent_id'].astype(str).str.strip().ne('') & df['agent_id'].notna() & (df['agent_id'] != None)
    df_with_id = df[valid_mask]
    df_without_id = df[~valid_mask]
    df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='agent_id', keep='first')
    df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)
    df_cleaned = df_cleaned.drop(columns=['non_empty_count'])
    df_cleaned.columns = df_cleaned.columns.str.lower()

    df_cleaned["hire_date"] = pd.to_datetime(df_cleaned["hire_date"], errors='coerce')
    df_cleaned["hire_date"] = df_cleaned["hire_date"].dt.strftime('%Y%m%d')
    df_cleaned["hire_date"] = df_cleaned["hire_date"].where(df_cleaned["hire_date"].notna(), None)
    df_cleaned["hire_date"] = df_cleaned["hire_date"].astype('Int64')

    df_cleaned["zipcode"] = df_cleaned["zipcode"].where(df_cleaned["zipcode"].str.len() == 5, None)
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
        if email is None or email == '' or str(email).strip() == '':
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
        if name is None or name == '' or str(name).strip() == '':
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
        if store_name is None or store_name == '' or str(store_name).strip() == '':
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
        if text is None or text == '' or str(text).strip() == '':
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
        if zipcode is None or zipcode == '' or str(zipcode).strip() == '':
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
        if address is None or address == '' or str(address).strip() == '':
            return None
        
        address_str = str(address).strip()
        
        # ลบสระพิเศษที่อยู่ด้านหน้าข้อความ (สระที่ไม่ได้เป็นส่วนของที่อยู่จริง)
        # ลบเครื่องหมาย -, :, . และตัวอักษรพิเศษ
        # ใช้ regex ที่ครอบคลุมสระทั้งหมดที่อยู่ด้านหน้า
        cleaned_address = re.sub(r'^[\u0E30-\u0E3A\u0E47-\u0E4E]+', '', address_str)  # ลบสระด้านหน้า
        cleaned_address = re.sub(r'[-:.,]', '', cleaned_address)  # ลบเครื่องหมายพิเศษ
        
        # ✅ ลบคำว่า "undefined" (ไม่คำนึงถึงตัวพิมพ์เล็ก-ใหญ่)
        cleaned_address = re.sub(r'\bundefined\b', '', cleaned_address, flags=re.IGNORECASE)
        
        # ลบช่องว่างที่ซ้ำกัน
        cleaned_address = re.sub(r'\s+', ' ', cleaned_address)
        
        # ลบช่องว่างที่ต้นและท้าย
        cleaned_address = cleaned_address.strip()
        
        # ถ้าหลังจากทำความสะอาดแล้วเป็นช่องว่างเปล่า ให้ return None
        if cleaned_address == '':
            return None
        
        return cleaned_address
    
    df_cleaned["agent_address"] = df_cleaned["agent_address"].apply(clean_address)
    
    # ✅ ทำความสะอาด agent_address column - แปลง NaN และ None เป็น None
    df_cleaned["agent_address"] = df_cleaned["agent_address"].where(pd.notna(df_cleaned["agent_address"]), None)
    df_cleaned["agent_address"] = df_cleaned["agent_address"].replace('NaN', None)
    df_cleaned["agent_address"] = df_cleaned["agent_address"].replace('None', None)
    df_cleaned["agent_address"] = df_cleaned["agent_address"].replace('NULL', None)

    # ✅ ลบ space ที่อยู่ด้านหน้าข้อความในทุกคอลัมน์
    def clean_leading_spaces(text):
        if text is None or text == '' or str(text).strip() == '':
            return None
        
        text_str = str(text).strip()
        # ลบ space ที่อยู่ด้านหน้าข้อความ
        cleaned_text = re.sub(r'^\s+', '', text_str)
        
        return cleaned_text if cleaned_text != '' else None
    
    # ใช้ฟังก์ชันกับทุกคอลัมน์
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':  # เฉพาะคอลัมน์ที่เป็นข้อความ
            df_cleaned[col] = df_cleaned[col].apply(clean_leading_spaces)

    # ✅ ตรวจสอบและแปลงข้อมูลสุดท้าย - แปลง pandas NaN เป็น None ในทุกคอลัมน์
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':
            df_cleaned[col] = df_cleaned[col].where(pd.notna(df_cleaned[col]), None)
            df_cleaned[col] = df_cleaned[col].replace('NaN', None)
            df_cleaned[col] = df_cleaned[col].replace('nan', None)
            df_cleaned[col] = df_cleaned[col].replace('NULL', None)
            df_cleaned[col] = df_cleaned[col].replace('null', None)
    
    print("\n📊 Cleaning completed")
    print(f"📊 Final data shape: {df_cleaned.shape}")
    print(f"📊 Final NaN/None count: {df_cleaned.isna().sum().sum()}")

    return df_cleaned

@op
def load_sales_data(df: pd.DataFrame):
    table_name = 'dim_sales'
    pk_column = 'agent_id'
    
    # ✅ ตรวจสอบและแปลงข้อมูลก่อน load - แปลง pandas NaN เป็น None
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].where(pd.notna(df[col]), None)
            df[col] = df[col].replace('NaN', None)
            df[col] = df[col].replace('nan', None)
            df[col] = df[col].replace('NULL', None)
            df[col] = df[col].replace('null', None)
    
    print(f"🔍 Before load - NaN/None count: {df.isna().sum().sum()}")
    print(f"🔍 Before load - job column NaN/None count: {df['job'].isna().sum()}")
    print(f"🔍 Before load - agent_address column NaN/None count: {df['agent_address'].isna().sum()}")

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
                if val_new is None and val_old is None:
                    continue
                elif val_new is None or val_old is None:
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
        
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna() & (df_to_insert[pk_column] != None)].copy()
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
                    c.name: stmt.excluded[c.name]
                    for c in metadata_table.columns
                    if c.name not in pk_column + ['id_contact', 'create_at', 'update_at']
                }
                # update_at ให้เป็นเวลาปัจจุบัน
                update_columns['update_at'] = datetime.now()
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")

@job
def dim_sales_etl():
    load_sales_data(clean_sales_data(extract_sales_data()))


if __name__ == "__main__":
    df_raw = extract_sales_data()
    print("✅ Extracted logs:", df_raw.shape)
    
    # ตรวจสอบข้อมูล NaN และ None
    print("🔍 Checking for NaN/None values in raw data...")
    nan_count = df_raw.isna().sum().sum()
    print(f"   - Total NaN/None values: {nan_count}")
    
    # ตรวจสอบข้อมูลที่เป็น string 'NaN', 'nan', 'NULL', 'null'
    string_nan_count = 0
    for col in df_raw.columns:
        if df_raw[col].dtype == 'object':
            string_nan_count += (df_raw[col] == 'NaN').sum()
            string_nan_count += (df_raw[col] == 'nan').sum()
            string_nan_count += (df_raw[col] == 'NULL').sum()
            string_nan_count += (df_raw[col] == 'null').sum()
    
    print(f"   - String NaN/NULL values: {string_nan_count}")
    
    # ตรวจสอบข้อมูลที่เป็น None object
    none_count = 0
    for col in df_raw.columns:
        if df_raw[col].dtype == 'object':
            none_count += (df_raw[col] == None).sum()
    
    print(f"   - None object values: {none_count}")

    df_clean = clean_sales_data((df_raw))
    print("✅ Cleaned columns:", df_clean.columns)
    
    # ตรวจสอบข้อมูล NaN และ None หลังจากทำความสะอาด
    print("🔍 Checking for NaN/None values in cleaned data...")
    nan_count_clean = df_clean.isna().sum().sum()
    print(f"   - Total NaN/None values: {nan_count_clean}")
    
    # ตรวจสอบข้อมูลที่เป็น string 'NaN', 'nan', 'NULL', 'null' หลังจากทำความสะอาด
    string_nan_count_clean = 0
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            string_nan_count_clean += (df_clean[col] == 'NaN').sum()
            string_nan_count_clean += (df_clean[col] == 'nan').sum()
            string_nan_count_clean += (df_clean[col] == 'NULL').sum()
            string_nan_count_clean += (df_clean[col] == 'null').sum()
    
    print(f"   - String NaN/NULL values: {string_nan_count_clean}")
    
    # ตรวจสอบข้อมูลที่เป็น None object หลังจากทำความสะอาด
    none_count_clean = 0
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            none_count_clean += (df_clean[col] == None).sum()
    
    print(f"   - None object values: {none_count_clean}")

    # output_path = "dim_sales.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_sales_data(df_clean)
    print("🎉 completed! Data upserted to dim_sales.")


