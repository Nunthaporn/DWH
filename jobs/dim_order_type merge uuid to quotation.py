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
SELECT quo_num,type_insure,type_work, type_status , type_key , app_type, chanel_key
FROM fin_system_select_plan 
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
database = 'fininsurance_task'

engine = create_engine(f'mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}')

query = """
SELECT quo_num,worksend
FROM fin_order

"""

df1 = pd.read_sql(query, engine)
df1


# %%
df_merged = pd.merge(df, df1, on='quo_num', how='left')
df_merged

# %%
import pandas as pd

def fill_chanel_key(row):
    chanel_key = row['chanel_key']
    type_key = row['type_key']
    app_type = row['app_type']
    type_insure = row['type_insure']

    # ถ้า chanel_key มีค่าอยู่แล้ว
    if pd.notnull(chanel_key) and str(chanel_key).strip() != "":
        return chanel_key

    # ถ้าทั้ง type_key และ app_type ไม่ null
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

    # ถ้ามีแค่ type_key
    if pd.notnull(type_key) and (pd.isnull(app_type) or str(app_type).strip() == ""):
        if pd.notnull(type_insure) and str(type_insure).strip() != "":
            return f"{type_key} {type_insure}"
        else:
            return type_key

    # ถ้ามีแค่ app_type
    if pd.notnull(app_type) and (pd.isnull(type_key) or str(type_key).strip() == ""):
        if pd.notnull(type_insure) and str(type_insure).strip() != "":
            return f"{app_type} {type_insure}"
        else:
            return app_type

    # ไม่มีอะไรเลย
    return None

# apply กลับ
df_merged['chanel_key'] = df_merged.apply(fill_chanel_key, axis=1)
df_merged

# %%
df_merged['chanel_key'] = df_merged['chanel_key'].replace({
    'B2B': 'APP B2B',
    'WEB ตรอ': 'WEB VIF',
    'TELE': 'APP TELE',
    'APP-B2C': 'APP B2C',
    'APP ประกันรถ' : 'APP B2B',
    'WEB ประกันรถ': 'WEB'
})
df_merged

# %%
df_merged.drop(columns=['type_key', 'app_type'], inplace=True)
df_merged

# %%
df_merged.rename(columns={
    "quo_num": "quotation_num",
    "type_insure": "type_insurance",
    "type_work": "order_type",
    "type_status": "check_type",
    "worksend": "work_type",
    "chanel_key": "key_channel"
}, inplace=True)
df_merged

# %%
import numpy as np

df_merged = df_merged.replace(r'^\s*$', np.nan, regex=True)

df_merged = df_merged.where(pd.notnull(df_merged), None)
df_merged

# %%
import numpy as np

# แปลงทุกคอลัมน์ใน df_merged1 ถ้ามีค่าเป็น string "NAN" ให้เปลี่ยนเป็น np.nan
df_merged = df_merged.replace("NaN", np.nan)
df_merged

# %%
df_merged = df_merged.drop_duplicates(subset=['quotation_num'], keep='first')
df_merged

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
FROM dim_order_type 
"""

df5 = pd.read_sql(query, engine)
df5

# %%
df5 = df5.drop(columns=['create_at', 'update_at'])
df5

# %%
df_result = pd.merge(df_merged, df5, on=['quotation_num'], how='right')
df_result

# %%
df_result = df_result[['quotation_num', 'order_type_id']]
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
df6 = df6.drop(columns=['create_at', 'update_at', 'order_type_id'])
df6

# %%
df_result1 = pd.merge(df_result, df6, on=['quotation_num'], how='right')
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
from sqlalchemy import text

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
            # ตรวจสอบว่ามี quotation_num และ order_type_id หรือไม่
            if 'quotation_num' not in record or pd.isna(record['quotation_num']):
                print(f"⚠️ Skip row: no quotation_num: {record}")
                continue
            if 'order_type_id' not in record or pd.isna(record['order_type_id']):
                print(f"⚠️ Skip row: no order_type_id: {record}")
                continue

            # ✅ Update เฉพาะคอลัมน์เดียว
            stmt = (
                update(table)
                .where(table.c.quotation_num == record['quotation_num'])
                .values(order_type_id=record['order_type_id'])
            )
            conn.execute(stmt)

print("✅ Update order_type_id completed successfully.")

# 🔥 ลบคอลัมน์ quotation_num ในตาราง dim_order_type
with engine.begin() as conn:
    conn.execute(text("ALTER TABLE dim_order_type DROP COLUMN quotation_num;"))

print("🗑️ ลบคอลัมน์ quotation_num ในตาราง dim_order_type เรียบร้อยแล้ว!")

# %%



