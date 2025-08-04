from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import OperationalError, DisconnectionError
from datetime import datetime, timedelta


# ✅ Load .env
load_dotenv()

# ✅ Source DB connections with timeout and retry settings
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_timeout=30,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300,
        'charset': 'utf8mb4',
        'autocommit': True
    }
)

task_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_timeout=30,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300,
        'charset': 'utf8mb4',
        'autocommit': True
    }
)

# ✅ Target PostgreSQL
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

def remove_commas_from_numeric(value):
    """ลบลูกน้ำออกจากค่าตัวเลข"""
    if pd.isna(value) or value is None:
        return value
    
    # แปลงเป็น string ก่อน
    value_str = str(value).strip()
    
    # ลบลูกน้ำออก
    value_str = value_str.replace(',', '')
    
    # ลบช่องว่าง
    value_str = value_str.strip()
    
    # ตรวจสอบว่าเป็นตัวเลขหรือไม่
    if value_str == '' or value_str.lower() in ['nan', 'none', 'null']:
        return None
    
    # ตรวจสอบว่าเป็นตัวเลขหรือไม่ (รวมทศนิยม)
    if re.match(r'^-?\d*\.?\d+$', value_str):
        return value_str
    
    return value

def clean_insurance_company(company):
    """ทำความสะอาดชื่อบริษัทประกัน - ลบตัวเลขและภาษาอังกฤษออกจากทุกตำแหน่ง เก็บแค่ภาษาไทย"""
    if pd.isna(company) or company is None:
        return None
    
    company_str = str(company).strip()
    
    # กรองข้อมูลที่ไม่ถูกต้อง - ลบข้อมูลที่มีสัญลักษณ์พิเศษหรือ SQL injection
    invalid_patterns = [
        r'[<>"\'\`\\]',  # SQL injection characters
        r'\.\./',  # path traversal
        r'[\(\)\{\}\[\]]+',  # brackets
        r'[!@#$%^&*+=|]+',  # special characters
        r'XOR',  # SQL injection
        r'if\(',  # SQL injection
        r'now\(\)',  # SQL injection
        r'\$\{',  # template injection
        r'\?\?\?\?',  # multiple question marks
        r'[0-9]+[XO][0-9]+',  # XSS patterns
    ]
    
    for pattern in invalid_patterns:
        if re.search(pattern, company_str, re.IGNORECASE):
            return None
    
    # ตรวจสอบความยาว
    if len(company_str) < 2 or len(company_str) > 100:
        return None
    
    # ตรวจสอบว่ามีตัวอักษรไทยหรือไม่ (ต้องมีอย่างน้อย 1 ตัว)
    if not re.search(r'[ก-๙]', company_str):
        return None
    
    # ลบตัวเลขและภาษาอังกฤษออกจากทุกตำแหน่ง
    # ใช้ regex เพื่อลบตัวเลข, ตัวอักษรภาษาอังกฤษ, และช่องว่างที่ติดกัน
    cleaned_company = re.sub(r'[0-9a-zA-Z\s]+', ' ', company_str)
    
    # ลบช่องว่างที่เหลือและช่องว่างที่ติดกัน
    cleaned_company = re.sub(r'\s+', ' ', cleaned_company).strip()
    
    # ตรวจสอบว่าหลังจากลบแล้วยังมีความยาวที่เหมาะสมหรือไม่
    if len(cleaned_company) < 2:
        return None
    
    # ตรวจสอบว่าหลังจากลบแล้วยังมีตัวอักษรไทยหรือไม่
    if not re.search(r'[ก-๙]', cleaned_company):
        return None
    
    return cleaned_company

def execute_query_with_retry(engine, query, max_retries=3, delay=5):
    """Execute query with retry mechanism for connection issues"""
    for attempt in range(max_retries):
        try:
            print(f"🔄 Attempt {attempt + 1}/{max_retries} - Executing query...")
            df = pd.read_sql(query, engine)
            print(f"✅ Query executed successfully on attempt {attempt + 1}")
            return df
        except (OperationalError, DisconnectionError) as e:
            if "Lost connection" in str(e) or "connection was forcibly closed" in str(e):
                print(f"⚠️ Connection lost on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"⏳ Waiting {delay} seconds before retry...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                    # Dispose and recreate engine to force new connections
                    engine.dispose()
                else:
                    print(f"❌ All retry attempts failed")
                    raise
            else:
                raise
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            raise

def close_engines():
    """Safely close all database engines"""
    try:
        source_engine.dispose()
        task_engine.dispose()
        target_engine.dispose()
        print("🔒 Database connections closed safely")
    except Exception as e:
        print(f"⚠️ Warning: Error closing connections: {str(e)}")

@op
def extract_motor_data():
    now = datetime.now()

    start_time = now.replace(minute=0, second=0, microsecond=0)
    end_time = now.replace(minute=59, second=59, microsecond=999999)

    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    plan_query = """
        SELECT quo_num, company, company_prb, assured_insurance_capital1, is_addon, type, repair_type
        FROM fin_system_select_plan
        WHERE datestart BETWEEN '{start_str}' AND '{end_str}' 
        AND type_insure = 'ประกันรถ'
    """
    df_plan = execute_query_with_retry(source_engine, plan_query)

    # Query 2: Extract order data with retry mechanism
    order_query = """
        SELECT quo_num, responsibility1, responsibility2, responsibility3, responsibility4,
               damage1, damage2, damage3, damage4, protect1, protect2, protect3, protect4,
               IF(sendtype = 'ที่อยู่ใหม่', provincenew, province) AS delivery_province,
               show_ems_price, show_ems_type
        FROM fin_order
    """
    df_order = execute_query_with_retry(task_engine, order_query)

    # Query 3: Extract payment data with retry mechanism
    pay_query = """
        SELECT quo_num, date_warranty, date_exp
        FROM fin_system_pay
        WHERE datestart BETWEEN '{start_str}' AND '{end_str}'
    """
    df_pay = execute_query_with_retry(source_engine, pay_query)

    print("📦 df_plan:", df_plan.shape)
    print("📦 df_order:", df_order.shape)
    print("📦 df_pay:", df_pay.shape)

    # Close source connections after extraction
    try:
        source_engine.dispose()
        task_engine.dispose()
        print("🔒 Source database connections closed")
    except Exception as e:
        print(f"⚠️ Warning: Error closing source connections: {str(e)}")

    return df_plan, df_order, df_pay

@op
def clean_motor_data(data_tuple):
    df_plan, df_order, df_pay = data_tuple

    df = df_plan.merge(df_order, on="quo_num", how="left")
    df = df.merge(df_pay, on="quo_num", how="left")

    def combine_columns(a, b):
        a_str = str(a).strip() if pd.notna(a) else ''
        b_str = str(b).strip() if pd.notna(b) else ''
        if a_str == '' and b_str == '':
            return ''
        elif a_str == b_str:
            return a_str
        elif a_str == '':
            return b_str
        elif b_str == '':
            return a_str
        return f"{a_str} {b_str}"

    df = df.rename(columns={
        "quo_num": "quotation_num",
        "company": "ins_company",
        "company_prb": "act_ins_company",
        "assured_insurance_capital1": "sum_insured",
        "is_addon": "income_comp_ins",
        "responsibility1": "human_coverage_person",
        "responsibility2": "human_coverage_atime",
        "responsibility3": "property_coverage",
        "responsibility4": "deductible",
        "damage1": "vehicle_damage",
        "damage2": "deductible_amount",
        "damage3": "vehicle_theft_fire",
        "damage4": "vehicle_flood_damage",
        "protect1": "personal_accident_driver",
        "protect2": "personal_accident_passengers",
        "protect3": "medical_coverage",
        "protect4": "driver_coverage",
        "show_ems_price": "ems_amount",
        "show_ems_type": "delivery_type",
        "date_warranty": "date_warranty",
        "date_exp": "date_expired",
        "type":"insurance_class"
    })

    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df["non_empty_count"] = df_temp.notnull().sum(axis=1)
    df = df.sort_values("non_empty_count", ascending=False).drop_duplicates(subset="quotation_num")
    df = df.drop(columns=["non_empty_count"], errors="ignore")

    df.columns = df.columns.str.lower()
    df["income_comp_ins"] = df["income_comp_ins"].apply(lambda x: True if x == 1 else False if x == 0 else None)
    # ทำความสะอาดคอลัมน์ ins_company
    if 'ins_company' in df.columns:
        before_count = df['ins_company'].notna().sum()
        # เก็บตัวอย่างข้อมูลที่ไม่ถูกต้องก่อนกรอง
        invalid_companies = df[df['ins_company'].notna()]['ins_company'].unique()
        df['ins_company'] = df['ins_company'].apply(clean_insurance_company)
        after_count = df['ins_company'].notna().sum()
        filtered_count = before_count - after_count
        print(f"🧹 Cleaned ins_company column - filtered {filtered_count} invalid records")
        if filtered_count > 0:
            print(f"   Examples of invalid companies: {list(invalid_companies[:5])}")
    
    # แปลง datetime และจัดการกับ NaT values
    df["date_warranty"] = pd.to_datetime(df["date_warranty"], errors="coerce")
    df["date_expired"] = pd.to_datetime(df["date_expired"], errors="coerce")
    
    # แปลง NaT เป็น None เพื่อให้ PostgreSQL เข้าใจ
    df["date_warranty"] = df["date_warranty"].replace({pd.NaT: None})
    df["date_expired"] = df["date_expired"].replace({pd.NaT: None})

    df['date_warranty'] = pd.to_datetime(df['date_warranty'], errors='coerce')
    df['date_warranty'] = df['date_warranty'].dt.strftime('%Y%m%d').astype('Int64')
    df['date_expired'] = pd.to_datetime(df['date_expired'], errors='coerce')
    df['date_expired'] = df['date_expired'].dt.strftime('%Y%m%d').astype('Int64')
    
    df['ems_amount'] = df['ems_amount'].fillna(0).astype(int)
    
    # ทำความสะอาดข้อมูลจังหวัด - เก็บแค่จังหวัดเท่านั้น
    def clean_province(province):
        if pd.isna(province) or str(province).strip() == '':
            return None
        
        province_str = str(province).strip()
        
        # ลบคำที่ไม่ใช่ชื่อจังหวัด
        remove_words = ['จังหวัด', 'อำเภอ', 'ตำบล', 'เขต', 'เมือง', 'กิโลเมตร', 'กม.', 'ถนน', 'ซอย', 'หมู่', 'บ้าน']
        for word in remove_words:
            province_str = province_str.replace(word, '').strip()
        
        # แก้ไขการสะกดผิดที่พบบ่อย
        corrections = {
            'กรุงเทพ': 'กรุงเทพมหานคร',
            'กรุงเทพฯ': 'กรุงเทพมหานคร',
            'กทม': 'กรุงเทพมหานคร',
            'กทม.': 'กรุงเทพมหานคร',
            'เชียงใหม่': 'เชียงใหม่',
            'ชลบุรี': 'ชลบุรี',
            'นนทบุรี': 'นนทบุรี',
            'ปทุมธานี': 'ปทุมธานี',
            'สมุทรปราการ': 'สมุทรปราการ',
            'สมุทรสาคร': 'สมุทรสาคร',
            'นครปฐม': 'นครปฐม',
            'นครราชสีมา': 'นครราชสีมา',
            'ขอนแก่น': 'ขอนแก่น',
            'อุบลราชธานี': 'อุบลราชธานี',
            'สุราษฎร์ธานี': 'สุราษฎร์ธานี',
            'สงขลา': 'สงขลา',
            'ภูเก็ต': 'ภูเก็ต',
            'พัทยา': 'ชลบุรี',
            'ศรีราชา': 'ชลบุรี',
            'บางนา': 'สมุทรปราการ',
            'บางพลี': 'สมุทรปราการ',
            'พระประแดง': 'สมุทรปราการ',
            'บางบ่อ': 'สมุทรปราการ',
            'บางเสาธง': 'สมุทรปราการ'
        }
        
        # ตรวจสอบการแก้ไข
        for wrong, correct in corrections.items():
            if wrong in province_str:
                return correct
        
        # ถ้าไม่เจอในรายการจังหวัดที่รู้จัก ให้เป็น None
        known_provinces = [
            'กรุงเทพมหานคร', 'เชียงใหม่', 'ชลบุรี', 'นนทบุรี', 'ปทุมธานี', 
            'สมุทรปราการ', 'สมุทรสาคร', 'นครปฐม', 'นครราชสีมา', 'ขอนแก่น',
            'อุบลราชธานี', 'สุราษฎร์ธานี', 'สงขลา', 'ภูเก็ต', 'เชียงราย',
            'ลำปาง', 'ลำพูน', 'แพร่', 'น่าน', 'พะเยา', 'แม่ฮ่องสอน',
            'ตาก', 'สุโขทัย', 'พิษณุโลก', 'เพชรบูรณ์', 'พิจิตร',
            'กำแพงเพชร', 'อุทัยธานี', 'นครสวรรค์', 'ลพบุรี', 'สิงห์บุรี',
            'ชัยนาท', 'สระบุรี', 'พระนครศรีอยุธยา', 'อ่างทอง', 'สุพรรณบุรี',
            'นครนายก', 'สระแก้ว', 'จันทบุรี', 'ตราด', 'ฉะเชิงเทรา',
            'ปราจีนบุรี', 'นครนายก', 'สระแก้ว', 'จันทบุรี', 'ตราด',
            'ฉะเชิงเทรา', 'ปราจีนบุรี', 'นครนายก', 'สระแก้ว', 'จันทบุรี',
            'ตราด', 'ฉะเชิงเทรา', 'ปราจีนบุรี', 'นครนายก', 'สระแก้ว',
            'จันทบุรี', 'ตราด', 'ฉะเชิงเทรา', 'ปราจีนบุรี', 'นครนายก',
            'สระแก้ว', 'จันทบุรี', 'ตราด', 'ฉะเชิงเทรา', 'ปราจีนบุรี'
        ]
        
        # ตรวจสอบว่าคือจังหวัดที่รู้จักหรือไม่
        for known in known_provinces:
            if known in province_str or province_str in known:
                return known
        
        return None
    
    # ทำความสะอาดคอลัมน์ delivery_province
    if 'delivery_province' in df.columns:
        df['delivery_province'] = df['delivery_province'].apply(clean_province)
        print(f"🧹 Cleaned delivery_province column - kept only provinces")
    
    # ทำความสะอาดคอลัมน์ delivery_type
    if 'delivery_type' in df.columns:
        df['delivery_type'] = df['delivery_type'].replace('nor', 'normal')
        print(f"📦 Cleaned delivery_type column - changed 'nor' to 'normal'")

    # ✅ ทำความสะอาดคอลัมน์ insurance_class (type) - เก็บแค่ค่าที่กำหนดเท่านั้น
    if 'insurance_class' in df.columns:
        valid_classes = ['1', '1+', '2', '2+', '3', '3+', 'พรบ']
        before_count = df['insurance_class'].notna().sum()
        
        # แปลงเป็น string และทำความสะอาด
        df['insurance_class'] = df['insurance_class'].astype(str).str.strip()
        
        # เก็บแค่ค่าที่ถูกต้องเท่านั้น
        df['insurance_class'] = df['insurance_class'].apply(
            lambda x: x if x in valid_classes else None
        )
        
        after_count = df['insurance_class'].notna().sum()
        filtered_count = before_count - after_count
        print(f"🧹 Cleaned insurance_class column - filtered {filtered_count} invalid records")
        print(f"   Valid classes: {valid_classes}")
        
        # แสดงสถิติค่าที่ถูกต้อง
        valid_counts = df['insurance_class'].value_counts()
        print(f"   Valid class distribution:")
        for class_name, count in valid_counts.items():
            print(f"     {class_name}: {count}")

    # ✅ ทำความสะอาดคอลัมน์ repair_type - เก็บแค่ภาษาไทยเท่านั้น
    if 'repair_type' in df.columns:
        def clean_repair_type(repair_type):
            """ทำความสะอาด repair_type - เก็บแค่ภาษาไทยเท่านั้น"""
            if pd.isna(repair_type) or repair_type is None:
                return None
            
            repair_str = str(repair_type).strip()
            
            # เก็บแค่ตัวอักษรภาษาไทยเท่านั้น
            thai_only = re.sub(r'[^ก-๙]', '', repair_str)
            
            # ตรวจสอบว่ามีตัวอักษรไทยหรือไม่
            if len(thai_only) < 2:
                return None
            
            # แก้ไขชื่อที่ผิดให้ถูกต้อง
            if thai_only in ['อู่', 'ซ่อมอู้', 'ซ่อมอู๋']:
                return 'ซ่อมอู่'
            elif thai_only in ['ซ่องห้าง', 'ซ่อมศูนย์']:
                return 'ซ่อมห้าง'
            
            return thai_only
        
        before_count = df['repair_type'].notna().sum()
        # เก็บตัวอย่างข้อมูลที่ไม่ถูกต้องก่อนกรอง
        invalid_repairs = df[df['repair_type'].notna()]['repair_type'].unique()
        df['repair_type'] = df['repair_type'].apply(clean_repair_type)
        after_count = df['repair_type'].notna().sum()
        filtered_count = before_count - after_count
        print(f"🧹 Cleaned repair_type column - filtered {filtered_count} invalid records")
        if filtered_count > 0:
            print(f"   Examples of invalid repair types: {list(invalid_repairs[:5])}")
        
        # แสดงสถิติข้อมูลที่ถูกต้อง
        valid_repairs = df['repair_type'].value_counts().head(10)
        print(f"   Top 10 valid repair types:")
        for repair_type, count in valid_repairs.items():
            print(f"     {repair_type}: {count}")

    # ✅ ลบลูกน้ำออกจากคอลัมน์ตัวเลขก่อนแปลงเป็นตัวเลข
    numeric_columns = [
        "sum_insured", "human_coverage_person", "human_coverage_atime", "property_coverage",
        "deductible", "vehicle_damage", "deductible_amount", "vehicle_theft_fire",
        "vehicle_flood_damage", "personal_accident_driver", "personal_accident_passengers",
        "medical_coverage", "driver_coverage", "ems_amount"
    ]

    print("🧹 Removing commas from numeric columns...")
    for col in numeric_columns:
        if col in df.columns:
            # ลบลูกน้ำออกจากข้อมูลก่อน
            df[col] = df[col].apply(remove_commas_from_numeric)
            
            # แปลงเป็นตัวเลขและจัดการกับ NaN values
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # ใช้ float64 แทน Int64 เพื่อหลีกเลี่ยงปัญหา casting
            df[col] = df[col].astype("float64")
    
    # ✅ แก้ไขค่า human_coverage_atime จาก 100,000,000 เป็น 10,000,000
    if 'human_coverage_atime' in df.columns:
        df['human_coverage_atime'] = df['human_coverage_atime'].replace(100000000, 10000000)
        print(f"🔧 Fixed human_coverage_atime: changed 100,000,000 to 10,000,000")
    
    # ✅ แก้ไขค่า vehicle_damage และ vehicle_theft_fire จาก 190000050 เป็น 1,900,000
    if 'vehicle_damage' in df.columns:
        df['vehicle_damage'] = df['vehicle_damage'].replace(190000050, 1900000)
        print(f"🔧 Fixed vehicle_damage: changed 190,000,050 to 1,900,000")
    
    if 'vehicle_theft_fire' in df.columns:
        df['vehicle_theft_fire'] = df['vehicle_theft_fire'].replace(190000050, 1900000)
        print(f"🔧 Fixed vehicle_theft_fire: changed 190,000,050 to 1,900,000")
    
    # ✅ แก้ไขค่า sum_insured จาก 190000050 เป็น 1900000 และ 250000093 เป็น 2500000
    if 'sum_insured' in df.columns:
        df['sum_insured'] = df['sum_insured'].replace(190000050, 1900000)
        df['sum_insured'] = df['sum_insured'].replace(250000093, 2500000)
        print(f"🔧 Fixed sum_insured: corrected abnormal values")
    
    df = df.where(pd.notnull(df), None)

    # แสดงสถิติข้อมูลที่ถูกต้อง
    if 'ins_company' in df.columns:
        valid_companies = df['ins_company'].value_counts().head(10)
        print(f"\n📊 Top 10 valid insurance companies:")
        for company, count in valid_companies.items():
            print(f"   {company}: {count}")
    
    if 'insurance_class' in df.columns:
        valid_classes = df['insurance_class'].value_counts().head(10)
        print(f"\n📊 Top 10 valid insurance classes:")
        for class_name, count in valid_classes.items():
            print(f"   {class_name}: {count}")

    print("\n📊 Cleaning completed")

    # Close any remaining connections
    try:
        source_engine.dispose()
        task_engine.dispose()
        print("🔒 Remaining database connections closed")
    except Exception as e:
        print(f"⚠️ Warning: Error closing remaining connections: {str(e)}")

    return df

@op
def load_motor_data(df: pd.DataFrame):
    table_name = "fact_insurance_motor"
    pk_column = "quotation_num"

    df = df[df[pk_column].notna()].copy()
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=target_engine)


    # ✅ วันปัจจุบัน (เริ่มต้นเวลา 00:00:00)
    today_str = datetime.now().strftime('%Y-%m-%d')

    # ✅ Load เฉพาะข้อมูลวันนี้จาก PostgreSQL
    with target_engine.connect() as conn:
        existing_query = pd.read_sql(
            f"SELECT * FROM {table_name} WHERE update_at >= '{today_str}'",
            conn
        )
    df_existing = execute_query_with_retry(target_engine, existing_query)

    existing_ids = set(df_existing[pk_column])
    new_rows = df[~df[pk_column].isin(existing_ids)].copy()
    update_rows = df[df[pk_column].isin(existing_ids)].copy()

    print(f"🆕 Insert: {len(new_rows)} rows")
    print(f"🔄 Update: {len(update_rows)} rows")

    if not new_rows.empty:
        print(f"🔄 Inserting {len(new_rows)} new records...")
        try:
            with target_engine.begin() as conn:
                conn.execute(table.insert(), new_rows.to_dict(orient="records"))
            print(f"✅ Insert completed successfully")
        except Exception as e:
            print(f"❌ Insert failed: {str(e)}")
            raise

    if not update_rows.empty:
        print(f"🔄 Updating {len(update_rows)} existing records...")
        try:
            with target_engine.begin() as conn:
                for i, record in enumerate(update_rows.to_dict(orient="records")):
                    if i % 100 == 0:  # Progress indicator every 100 records
                        print(f"   Progress: {i}/{len(update_rows)} records processed")
                    
                    stmt = pg_insert(table).values(**record)
                    update_dict = {
                        c.name: stmt.excluded[c.name]
                        for c in table.columns if c.name not in [pk_column, 'create_at', 'update_at']
                    }
                    # update_at ให้เป็นเวลาปัจจุบัน
                    update_dict['update_at'] = datetime.now()
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[pk_column],
                        set_=update_dict
                    )
                    conn.execute(stmt)
            print(f"✅ Update completed successfully")
        except Exception as e:
            print(f"❌ Update failed: {str(e)}")
            raise

    print("✅ Insert/update completed.")

    # Close target engine connection after load
    try:
        target_engine.dispose()
        print("🔒 Target database connection closed")
    except Exception as e:
        print(f"⚠️ Warning: Error closing target connection: {str(e)}")

    # Close any remaining connections
    try:
        source_engine.dispose()
        task_engine.dispose()
        print("🔒 All database connections closed")
    except Exception as e:
        print(f"⚠️ Warning: Error closing all connections: {str(e)}")

@job
def fact_insurance_motor_etl():
    load_motor_data(clean_motor_data(extract_motor_data()))

# if __name__ == "__main__":
#     try:
#         print("🚀 Starting fact_insurance_motor ETL process...")
        
#         # Extract data with retry mechanism
#         print("📥 Extracting data from source databases...")
#         df_raw = extract_motor_data()
#         print("✅ Data extraction completed")

#         # Clean data
#         print("🧹 Cleaning and transforming data...")
#         df_clean = clean_motor_data((df_raw))
#         print("✅ Data cleaning completed")
#         print("✅ Cleaned columns:", df_clean.columns)

#         # Save to Excel for inspection
#         # output_path = "fact_insurance_motor.xlsx"
#         # df_clean.to_excel(output_path, index=False, engine='openpyxl')
#         # print(f"💾 Saved to {output_path}")

#         # Uncomment to load to database
#         print("📤 Loading data to target database...")
#         load_motor_data(df_clean)
#         print("🎉 ETL process completed! Data upserted to fact_insurance_motor.")
        
#     except Exception as e:
#         print(f"❌ ETL process failed: {str(e)}")
#         raise
#     finally:
#         # Always close database connections
#         close_engines()