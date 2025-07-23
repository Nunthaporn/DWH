# %%
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()  # อย่าลืมโหลด .env

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')  
database = 'fininsurance'

engine = create_engine(f'mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}')

query = """
SELECT quo_num, address, province, amphoe, district, zipcode, datestart
FROM fin_system_pay
WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
AND type_insure IN ('ประกันรถ', 'ตรอ')
"""

df = pd.read_sql(query, engine)
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
SELECT quo_num, idcard, title, name, lastname, birthDate, career, gender, tel, email, datestart
FROM fin_system_select_plan
WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
AND type_insure IN ('ประกันรถ', 'ตรอ')
"""

df1 = pd.read_sql(query, engine)
df1


# %%
df_merged = pd.merge(df, df1, on='quo_num', how='right')
df_merged

# %%
df_merged = df_merged.drop_duplicates(subset=['address', 'province', 'amphoe', 'district', 'zipcode'])
df_merged

# %%
df_merged = df_merged.drop_duplicates(subset=['name', 'lastname'])
df_merged

# %%
df_merged = df_merged.drop_duplicates(subset=['idcard'])
df_merged

# %%
df_merged = df_merged.drop(columns=['datestart_x', 'datestart_y'])
df_merged

# %%
df_merged['full_name'] = df_merged.apply(
    lambda row: row['name'] if str(row['name']).strip() == str(row['lastname']).strip()
    else f"{str(row['name']).strip()} {str(row['lastname']).strip()}",
    axis=1
)

df_merged

# %%
df_cleaned = df_merged.drop(columns=['name', 'lastname'])
df_cleaned

# %%
from datetime import date
import pandas as pd

# แปลง birth_date เป็น datetime (ถ้ายังไม่ได้แปลง)
df_cleaned['birthDate'] = pd.to_datetime(df_cleaned['birthDate'], errors='coerce')

# คำนวณอายุจาก birthDate และจัดการค่า null
df_cleaned['age'] = df_cleaned['birthDate'].apply(
    lambda x: (
        date.today().year - x.year - ((date.today().month, date.today().day) < (x.month, x.day))
        if pd.notnull(x) else pd.NA
    )
).astype('Int64')
df_cleaned

# %%
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
df

# %%
gender_mapping = {
    'M': 'Male',
    'F': 'Female',

}

df['customer_gender'] = df['customer_gender'].map(gender_mapping)


# %%
import numpy as np

df = df.replace(
    to_replace=r'^\s*$|^(?i:none|null|na)$|^[-.]$',
    value=np.nan,
    regex=True
)
df

# %%
# ลบคำว่า ' None' ที่อยู่หลังสุดของข้อความ
df['customer_name'] = df['customer_name'].str.replace(r'\s*None$', '', regex=True)
df

# %%
df['customer_telnumber'] = df['customer_telnumber'].str.replace('-', '', regex=False)
df

# %%
test_names = [
    'ทดสอบ',
    'ทดสอบ ',
    'ทดสอบ จากฟิน',
    'ทดสอบ พ.ร.บ.',
    'ทดสอบ06',
    'ทดสอบ',
    'ทดสอบระบบ ประกัน+พ.ร.บ.',
    'ลูกค้า ทดสอบ',
    'ทดสอบ เช็คเบี้ย',
    'ทดสอบพ.ร.บ. งานคีย์มือ',
    'ทดสอบ ระบบ',
    'ทดสอบคีย์มือ ธนชาตผู้ขับขี่',
    'ทดสอบ04',
    'test',
    'test2',
    'test tes',
    'test ระบบ',
    'Tes ระบบ'
]

df = df[~df['customer_name'].isin(test_names)]
df

# %%
import re

def clean_telnumber(val):
    if pd.isnull(val) or val.strip() == "":
        return None

    # ดึงเฉพาะตัวเลข
    digits = re.sub(r'\D', '', val)

    # ถ้าไม่เหลือตัวเลข → แปลงเป็น None
    if digits == "":
        return None
    else:
        return digits

# Apply
df['customer_telnumber'] = df['customer_telnumber'].apply(clean_telnumber)

df


# %%
df['address'] = df['address'].str.replace('-', '', regex=False)
df

# %%
import re

def clean_address(val):
    if pd.isnull(val) or val.strip() == "":
        return val

    # ลบตัวอักษรสระ/วรรณยุกต์ที่อยู่ต้น string
    # กลุ่มสระ/วรรณยุกต์: ุ ู ึ ื ี ฺ ำ ะ ั ิ ฯ ฦ ะ ฯลฯ
    # แต่ในกรณีนี้เราจะเน้นสระที่พิมพ์ผิด: ุ ู ึ ื ี ิ 
    cleaned = re.sub(r'^[ุูึืิ]+', '', val.strip())
    return cleaned

# Apply
df['address'] = df['address'].apply(clean_address)
df


# %%
import re

def remove_parentheses(val):
    if pd.isnull(val) or val.strip() == "":
        return val

    # ลบทุกอย่างในวงเล็บรวมวงเล็บ
    cleaned = re.sub(r'\([^)]*\)', '', val).strip()
    return cleaned

# Apply
df['address'] = df['address'].apply(remove_parentheses)

df


# %%
import pandas as pd
import re

def clean_address(val):
    if pd.isnull(val) or val.strip() == "":
        return None

    val = val.strip()

    # ถ้ามีแค่ "/" อย่างเดียว → ให้เป็น None
    if val == "/":
        return None

    # ลบ ":" ที่อยู่ต้นหรือที่มีอยู่
    val = val.lstrip(':').strip()

    return val

# Apply
df['address'] = df['address'].apply(clean_address)

df


# %%
df['customer_email'] = df['customer_email'].str.replace('_', '', regex=False)
df

# %%
df['title'] = df['title'].str.replace('‘นาย', 'นาย', regex=False).str.strip()
df

# %%
# df.to_excel('dim_customer.xlsx', index=False, engine='openpyxl')

# %% [markdown]
# db postgres update table quotation

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
FROM dim_customer 
"""

df5 = pd.read_sql(query, engine)
df5

# %%
df5 = df5.drop(columns=['create_at', 'update_at'])
df5

# %%
df_result = pd.merge(df, df5, on=['customer_card', 'customer_name'], how='right')
df_result

# %%
df_result = df_result[['quo_num', 'customer_sk']]
df_result

# %%
df_result = df_result.rename(columns={'quo_num': 'quotation_num'})
df_result

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
df6 = df6.drop(columns=['create_at', 'update_at', 'customer_id'])
df6

# %%
df_result1 = pd.merge(df_result, df6, on=['quotation_num'], how='right')
df_result1

# %%
df_result1 = df_result1.rename(columns={'customer_sk': 'customer_id'})
df_result1

# %%
df_result1 = df_result1.drop_duplicates(subset=['quotation_num'], keep='last')
df_result1

# %%
import numpy as np
import pandas as pd

# แก้ NaT, NaN ทั้ง dataframe ให้เป็น None
df_result1 = df_result1.where(pd.notnull(df_result1), None)


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

records = df_result1.to_dict(orient='records')

chunk_size = 5000

for start in range(0, len(records), chunk_size):
    end = start + chunk_size
    chunk = records[start:end]

    print(f"🔄 Updating chunk {start // chunk_size + 1}: records {start} to {end - 1}")

    with engine.begin() as conn:
        for record in chunk:
            # ตรวจสอบว่ามี quotation_num และ customer_id หรือไม่
            if 'quotation_num' not in record or pd.isna(record['quotation_num']):
                print(f"⚠️ Skip row: no quotation_num: {record}")
                continue
            if 'customer_id' not in record or pd.isna(record['customer_id']):
                print(f"⚠️ Skip row: no customer_id: {record}")
                continue

            # ✅ Update เฉพาะคอลัมน์เดียว
            stmt = (
                update(table)
                .where(table.c.quotation_num == record['quotation_num'])
                .values(customer_id=record['customer_id'])
            )
            conn.execute(stmt)

print("✅ Update customer_id completed successfully.")

# %%



