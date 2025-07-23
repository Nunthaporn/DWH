# %%
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import numpy as np

# โหลดตัวแปรจาก .env
load_dotenv()

# ดึงค่าจาก environment
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')  
database = 'fininsurance'

# สร้าง engine สำหรับเชื่อมต่อฐานข้อมูล
engine = create_engine(f'mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}')

# SQL query
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


# โหลดข้อมูลจากฐานข้อมูล
df = pd.read_sql(query, engine)

# แปลงให้ pandas เข้าใจได้แน่นอน
df['user_registered'] = pd.to_datetime(df['user_registered'].astype(str), errors='coerce')
df


# %%
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv() 

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')  
database = 'fininsurance'

engine = create_engine(f'mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}')

query = """
SELECT cuscode , career
FROM policy_register
"""

df1 = pd.read_sql(query, engine)
df1


# %%
df_merged = pd.merge(df, df1, on='cuscode', how='left')
df_merged

# %%
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


# %%
df_merged = df_merged.drop(columns=['fin_new_group','fin_new_mem'])
df_merged

# %%
df_merged['date_active'] = pd.to_datetime(df_merged['date_active'], errors='coerce')


# %%
import pandas as pd
from datetime import datetime, timedelta

# สมมติ df_cleaned คือ DataFrame ที่มี date_active และ status
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


# %%
df_merged = df_merged.drop(columns=['status','date_active'])

# %%
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
df

# %%
df['status_agent'].unique()


# %%
df['is_experienced_fix'] = df['is_experienced'].apply(lambda x: 'เคยขาย' if str(x).strip().lower() == 'ไม่เคยขาย' else 'ไม่เคยขาย')
df['is_experienced_fix']

# %%
df = df.drop(columns=['is_experienced'])

# %%
df.rename(columns={'is_experienced_fix': 'is_experienced'}, inplace=True)
df

# %%
valid_types = ['BUY', 'SELL', 'SHARE']

df.loc[~df['type_agent'].isin(valid_types), 'type_agent'] = np.nan

# %%
valid_rank = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']

df.loc[~df['agent_rank'].isin(valid_rank), 'agent_rank'] = np.nan


# %%
df['agent_rank'].unique()

# %%
import pandas as pd
import re

def clean_address(addr):
    if pd.isna(addr):
        return ''

    # ลบ 'เลขที่ -', 'หมู่ที่ -', 'หมู่บ้าน -', 'ซอย -', 'ถนน -'
    addr = re.sub(r'(เลขที่|หมู่ที่|หมู่บ้าน|ซอย|ถนน)[\s\-]*', '', addr, flags=re.IGNORECASE)

    # ลบ - เดี่ยวที่เหลืออยู่ (ซึ่งมักเป็นช่องว่างแฝง)
    addr = re.sub(r'\s*-\s*', '', addr)

    # ลบช่องว่างซ้ำกัน
    addr = re.sub(r'\s+', ' ', addr)

    # ตัดช่องว่างหัวท้าย
    return addr.strip()

# สมมติ df มีคอลัมน์ชื่อ 'address'
df['agent_address_cleaned'] = df['agent_address'].apply(clean_address)


# %%
df = df.drop(columns=['agent_address'])

# %%
df.rename(columns={'agent_address_cleaned': 'agent_address'}, inplace=True)


# %%
# df = df.replace(r'^\s*$', pd.NA, regex=True)  
# df = df[df.count(axis=1) > 1]
# df

# %%
import pandas as pd
import numpy as np

# แปลงช่องว่างทุกแบบเป็น NaN ชั่วคราว เพื่อการนับข้อมูล
df_temp = df.replace(r'^\s*$', np.nan, regex=True)

# เพิ่มคอลัมน์ช่วยนับจำนวนข้อมูล (non-null)
df['non_empty_count'] = df_temp.notnull().sum(axis=1)

# >>>> ส่วนที่แก้ไขตรงนี้ <<<<
# ตรวจสอบ agent_id ที่ไม่ว่าง (ไม่ใช่ NaN และไม่ใช่ช่องว่าง)
valid_agent_id_mask = df['agent_id'].astype(str).str.strip().ne('') & df['agent_id'].notna()

# แยกกลุ่มที่ agent_id ไม่ว่างและ agent_id ว่าง
df_with_id = df[valid_agent_id_mask]
df_without_id = df[~valid_agent_id_mask]

# คัดแถวที่ agent_id ซ้ำ โดยเก็บแถวที่มีข้อมูลมากที่สุด
df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='agent_id', keep='first')

# รวมกลับ
df_cleaned = pd.concat([df_with_id_cleaned, df_without_id], ignore_index=True)

# ลบคอลัมน์ช่วย
df_cleaned = df_cleaned.drop(columns=['non_empty_count'])
df_cleaned = df_cleaned.replace(
    to_replace=r'^\s*$|(?i:^none$)|^-$',  # << แก้ตรงนี้
    value=np.nan,
    regex=True
)


df_cleaned.columns = df_cleaned.columns.str.lower()
df_cleaned


# %%
df_cleaned.replace(np.nan, "NaN").isin(["none", "-", "None"]).sum()
df_cleaned

# %%
df_cleaned['is_experienced'] = df_cleaned['is_experienced'].apply(lambda x: 'yes' if str(x).strip().lower() == 'no' else 'no')


# %%
df_cleaned = df_cleaned.replace(r'^\.$', np.nan, regex=True)
df_cleaned

# %%
import numpy as np

# ฟังก์ชันช่วยแปลง
def clean_value(val):
    if pd.isna(val):         # NaN แบบจริง (เช่น np.nan)
        return None
    if isinstance(val, str):
        if val.strip() == "":
            return None
        if val.strip().lower() == "nan":
            return None
    return val

# ใช้ applymap แปลงทุก cell ในทั้ง DataFrame
df_cleaned = df_cleaned.applymap(clean_value)
df_cleaned


# %%
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv() 

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')  
database = 'fininsurance'

engine = create_engine(f'mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}')

query = """
SELECT quo_num, id_cus
FROM fin_system_select_plan
"""

df4 = pd.read_sql(query, engine)
df4


# %%
df4 = df4.rename(columns={'quo_num': 'quotation_num'})
df4

# %%
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import numpy as np

# โหลดตัวแปรจาก .env
load_dotenv()

# ดึงค่าจาก environment
user = os.getenv('DB_USER_test')
password = os.getenv('DB_PASSWORD_test')
host = os.getenv('DB_HOST_test')
port = os.getenv('DB_PORT_test')  
database = 'fininsurance'

# สร้าง engine สำหรับเชื่อมต่อฐานข้อมูล
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

# SQL query
query = """
SELECT *
FROM fact_sales_quotation 
"""

df6 = pd.read_sql(query, engine)
df6

# %%
df6 = df6.drop(columns=['create_at', 'update_at', 'agent_id'])
df6

# %%
df_result1 = pd.merge(df4, df6, on=['quotation_num'], how='right')
df_result1

# %%
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import numpy as np

# โหลดตัวแปรจาก .env
load_dotenv()

# ดึงค่าจาก environment
user = os.getenv('DB_USER_test')
password = os.getenv('DB_PASSWORD_test')
host = os.getenv('DB_HOST_test')
port = os.getenv('DB_PORT_test')  
database = 'fininsurance'

# สร้าง engine สำหรับเชื่อมต่อฐานข้อมูล
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

# SQL query
query = """
SELECT *
FROM dim_agent 
"""

df5 = pd.read_sql(query, engine)
df5

# %%
df5 = df5.drop(columns=['create_at', 'update_at'])
df5

# %%
df5 = df5.rename(columns={'agent_id': 'id_cus'})
df5

# %%
df_result = pd.merge(df_result1, df5, on=['id_cus'], how='inner')
df_result

# %%
df_result.columns

# %%
import numpy as np
import pandas as pd

# แก้ NaT, NaN ทั้ง dataframe ให้เป็น None
df_result = df_result.where(pd.notnull(df_result), None)


# %%
df_result = df_result.rename(columns={'id_contact': 'agent_id'})
df_result  


# %%
df_selected = df_result[['quotation_num', 'agent_id']]
df_selected 

# %%
import os
from sqlalchemy import create_engine, MetaData, Table, update

user = os.getenv('DB_USER_test')
password = os.getenv('DB_PASSWORD_test')
host = os.getenv('DB_HOST_test')
port = os.getenv('DB_PORT_test')
database = 'fininsurance'

engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

metadata = MetaData()
table = Table('fact_sales_quotation', metadata, autoload_with=engine)

records = df_selected.to_dict(orient='records')

chunk_size = 5000

for start in range(0, len(records), chunk_size):
    end = start + chunk_size
    chunk = records[start:end]

    print(f"🔄 Updating chunk {start // chunk_size + 1}: records {start} to {end - 1}")

    with engine.begin() as conn:
        for record in chunk:
            # ตรวจสอบว่ามี quotation_num และ agent_id หรือไม่
            if 'quotation_num' not in record or pd.isna(record['quotation_num']):
                print(f"⚠️ Skip row: no quotation_num: {record}")
                continue
            if 'agent_id' not in record or pd.isna(record['agent_id']):
                print(f"⚠️ Skip row: no agent_id: {record}")
                continue

            # ✅ Update เฉพาะคอลัมน์เดียว
            stmt = (
                update(table)
                .where(table.c.quotation_num == record['quotation_num'])
                .values(agent_id=record['agent_id'])
            )
            conn.execute(stmt)

print("✅ Update agent_id completed successfully.")

# %%
df_selected.to_excel('1.xlsx', index=False, engine='openpyxl')


