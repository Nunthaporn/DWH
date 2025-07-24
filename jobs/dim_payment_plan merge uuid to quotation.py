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

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

query = """
SELECT quo_num, type_insure
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
database = 'fininsurance'

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

query = """
SELECT quo_num, chanel_main, clickbank, chanel, numpay, condition_install
FROM fin_system_pay
WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
AND type_insure IN ('ประกันรถ', 'ตรอ')
"""

df2 = pd.read_sql(query, engine)
df2


# %%
# df2['numpay'] = df2['numpay'].replace({
#     0: 1
# })
# df2

# %%
df2['chanel'] = df2['chanel'].replace({
    'ผ่อนบัตร': 'เข้าฟิน'
})
df2

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

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

query = """
SELECT quo_num, status_paybill
FROM fininsurance_task.fin_order
WHERE type_insure IN ('ประกันรถ', 'ตรอ')
"""

df4 = pd.read_sql(query, engine)
df4


# %%
df_merged1 = pd.merge(df2, df4, on=['quo_num'],how='left')
df_merged1

# %%
# df_merged1['numpay'] = df_merged1['numpay'].replace({
#     0: 1
# })
# df_merged1

# %%
df_merged1 = df_merged1.rename(columns={'quo_num': 'quotation_num',
                          'type_insure': 'type_insurance',
                          'chanel': 'payment_reciever',
                          'status_paybill': 'payment_type',
                          })
df_merged1

# %%
def determine_payment_channel(row):
    ch_main = str(row['chanel_main']).strip().lower()
    cb_raw = row['clickbank']
    cb = str(cb_raw).strip().lower()

    is_cb_empty = pd.isna(cb_raw) or cb == ''

    # ✅ เงื่อนไขเฉพาะสำหรับตัดบัตร / ผ่อนบัตร
    if ch_main in ['ตัดบัตรเครดิต', 'ผ่อนบัตร', 'ผ่อนบัตรเครดิต', 'ผ่อนชำระ']:
        if 'qrcode' in cb:
            return 'QR Code'
        elif 'creditcard' in cb:
            return '2C2P'
        else:
            return 'ตัดบัตรกับฟิน'

    # ✅ เงื่อนไขสำหรับโอนเงิน / ผ่อนโอน
    if ch_main in ['โอนเงิน', 'ผ่อนโอน']:
        if 'qrcode' in cb:
            return 'QR Code'
        else:
            return 'โอนเงิน'

    # ✅ เงื่อนไขทั่วไป
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


# สร้างคอลัมน์ใหม่
df_merged1['payment_channel'] = df_merged1.apply(determine_payment_channel, axis=1)
df_merged1

# %%
df_merged1.drop(columns=['chanel_main', 'clickbank', 'condition_install'], inplace=True)
df_merged1

# %%
df_merged1 = df_merged1.rename(columns={'numpay': 'installment_number',

                          })
df_merged1

# %%
# df_merged3[df_merged3['quotation_num'].str.endswith('-r', na=False)]

# %%
df_merged1 = df_merged1[~df_merged1['quotation_num'].str.endswith('-r', na=False)]
df_merged1

# %%
import numpy as np

df_merged1 = df_merged1.replace(['', np.nan], None)
df_merged1

# %%
df_merged1['installment_number'] = df_merged1['installment_number'].replace({
    0: 1
})
df_merged1

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

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

query = """
SELECT * 
FROM fininsurance.fin_system_select_plan
where name in ('ทดสอบ','test')
and datestart >= '2025-05-01' AND datestart < '2025-07-01'
AND type_insure IN ('ประกันรถ', 'ตรอ');
"""

dele = pd.read_sql(query, engine)
dele


# %%
dele = dele.rename(columns={'quo_num': 'quotation_num', 'num_pay': 'installment_number'})
dele

# %%
# ลบแถวใน df ที่มี quo_num ตรงกับใน dele
df_merged1 = df_merged1[~df_merged1['quotation_num'].isin(dele['quotation_num'])]
df_merged1

# %%
df_merged1 = df_merged1[df_merged1['quotation_num'] != 'FQ2505-24999']
df_merged1

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
FROM dim_payment_plan 
"""

df5 = pd.read_sql(query, engine)
df5

# %%
df5 = df5.drop(columns=['create_at', 'update_at'])
df5

# %%
df_result = pd.merge(df_merged1, df5, on=['quotation_num'], how='right')
df_result

# %%
df_result = df_result[['quotation_num', 'payment_plan_id']]
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
df6 = df6.drop(columns=['create_at', 'update_at', 'payment_plan_id'])
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
            # ตรวจสอบว่ามี quotation_num และ payment_plan_id หรือไม่
            if 'quotation_num' not in record or pd.isna(record['quotation_num']):
                print(f"⚠️ Skip row: no quotation_num: {record}")
                continue
            if 'payment_plan_id' not in record or pd.isna(record['payment_plan_id']):
                print(f"⚠️ Skip row: no payment_plan_id: {record}")
                continue

            # ✅ Update เฉพาะคอลัมน์เดียว
            stmt = (
                update(table)
                .where(table.c.quotation_num == record['quotation_num'])
                .values(payment_plan_id=record['payment_plan_id'])
            )
            conn.execute(stmt)

print("✅ Update payment_plan_id completed successfully.")

# 🔥 ลบคอลัมน์ quotation_num ในตาราง dim_payment_plan
with engine.begin() as conn:
    conn.execute(text("ALTER TABLE dim_payment_plan DROP COLUMN quotation_num;"))

print("🗑️ ลบคอลัมน์ quotation_num ในตาราง dim_payment_plan เรียบร้อยแล้ว!")

# %%



