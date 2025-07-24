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
SELECT quo_num, id_motor1, id_motor2, datestart
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

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

query = """
SELECT quo_num, idcar, carprovince, camera, no_car, brandplan, seriesplan, sub_seriesplan, yearplan, detail_car, vehGroup, vehBodyTypeDesc, seatingCapacity, weight_car, cc_car, color_car, datestart
FROM fin_system_select_plan
WHERE datestart >= '2025-05-01' AND datestart < '2025-07-01'
AND type_insure IN ('ประกันรถ', 'ตรอ')
"""

df1 = pd.read_sql(query, engine)
df1


# %%
df_merged = pd.merge(df, df1, on='quo_num', how='left')
df_merged

# %%
df_merged = df_merged.drop_duplicates(subset=['id_motor2'])
df_merged

# %%
df_merged = df_merged.drop_duplicates(subset=['idcar'])
df_merged

# %%
# df_merged = df_merged.drop(columns=['datestart_x', 'datestart_y', 'quo_num'])
# df_merged

# %%
df_merged = df_merged.drop(columns=['datestart_x', 'datestart_y'])
df_merged

# %%
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
df

# %%
df = df.replace(r'^\s*$', pd.NA, regex=True)  
df = df[df.count(axis=1) > 1]
df

# %%
import pandas as pd
import numpy as np

# แปลงช่องว่างทุกแบบเป็น NaN ชั่วคราว เพื่อการนับข้อมูล
df_temp = df.replace(r'^\s*$', np.nan, regex=True)

# เพิ่มคอลัมน์ช่วยนับจำนวนข้อมูล (non-null)
df['non_empty_count'] = df_temp.notnull().sum(axis=1)

# >>>> ส่วนที่แก้ไขตรงนี้ <<<<
# ตรวจสอบ car_id ที่ไม่ว่าง (ไม่ใช่ NaN และไม่ใช่ช่องว่าง)
valid_car_id_mask = df['car_id'].astype(str).str.strip().ne('') & df['car_id'].notna()

# แยกกลุ่มที่ car_id ไม่ว่างและ car_id ว่าง
df_with_id = df[valid_car_id_mask]
df_without_id = df[~valid_car_id_mask]

# คัดแถวที่ car_id ซ้ำ โดยเก็บแถวที่มีข้อมูลมากที่สุด
df_with_id_cleaned = df_with_id.sort_values('non_empty_count', ascending=False).drop_duplicates(subset='car_id', keep='first')

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
df_cleaned = df_cleaned.replace(r'^\.$', np.nan, regex=True)
df_cleaned

# %%
import numpy as np

# แทนค่า "อื่นๆ" ด้วย np.nan
df_cleaned['seat_count'] = df_cleaned['seat_count'].replace("อื่นๆ", np.nan)

# ถ้าคอลัมน์นี้ควรเป็นตัวเลขด้วย แปลง type เป็น numeric (optional)
df_cleaned['seat_count'] = pd.to_numeric(df_cleaned['seat_count'], errors='coerce')
df_cleaned

# %%
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


# %%
import re

def extract_clean_plate(value):
    if pd.isnull(value) or value.strip() == "":
        return None

    text = value.strip()

    # ตัดด้วย / หรือ ///
    text = re.split(r'[\/]', text)[0].strip()

    # ตัดข้อความหลัง space
    parts = text.split()
    if len(parts) > 0:
        text = parts[0].strip()
    else:
        return None

    # ลบชื่อจังหวัด
    for prov in province_list:
        if prov in text:
            text = text.replace(prov, "").strip()

    # ใช้ regex ตัดเฉพาะทะเบียนจริง
    reg_match = re.match(r'^((?:\d{1,2})?[ก-ฮ]{1,3}\d{1,4})', text)
    if reg_match:
        final_plate = reg_match.group(1)

        # ลบขีด (ถ้ามี)
        final_plate = final_plate.replace('-', '')

        # ถ้าเริ่มด้วยเลข 2 ตัว → ตัดตัวแรก
        match_two_digits = re.match(r'^(\d{2})([ก-ฮ].*)$', final_plate)
        if match_two_digits:
            final_plate = match_two_digits.group(1)[1:] + match_two_digits.group(2)

        # หลังจากนั้น ถ้ายังขึ้นต้นด้วย 0 → ตัดออก
        if final_plate.startswith("0"):
            final_plate = final_plate[1:]

        return final_plate
    else:
        return None

# Apply ฟังก์ชันใหม่ไปที่คอลัมน์ car_registration
df_cleaned['car_registration'] = df_cleaned['car_registration'].apply(extract_clean_plate)

df_cleaned


# %%


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
FROM dim_car 
"""

df5 = pd.read_sql(query, engine)
df5

# %%
df5 = df5.drop(columns=['create_at', 'update_at'])
df5

# %%
df_cleaned['car_year'] = df_cleaned['car_year'].astype('Int64')
df5['car_year'] = df5['car_year'].astype('Int64')

# %%
import numpy as np

df5 = df5.replace(["NaN", "NAN", "nan"], np.nan)
df_cleaned = df_cleaned.replace(["NaN", "NAN", "nan"], np.nan)

# %%
fix_cols = ['vehicle_group', 'vehicle_weight', 'engine_capacity']

for col in fix_cols:
    df_cleaned[col] = df_cleaned[col].astype(str).replace('nan', pd.NA)
    df5[col] = df5[col].astype(str).replace('nan', pd.NA)


# %%
# df_concat = pd.concat([df_cleaned.reset_index(drop=True), df5[['car_sk']].reset_index(drop=True)], axis=1)
# df_concat

# %%
df_result = pd.merge(df_cleaned, df5, on=['car_id', 'car_registration'], how='right')
df_result

# %%
df_result = df_result[['quo_num', 'car_sk']]
df_result

# %%
df_result = df_result.rename(columns={'quo_num': 'quotation_num'})
df_result

# %%
# df_result.to_excel('test1.xlsx', index=False, engine='openpyxl')

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
df6 = df6.drop(columns=['create_at', 'update_at', 'car_id'])
df6

# %%
# df6 = df6.drop(columns=['car_id'])
# df6

# %%
df_result1 = pd.merge(df_result, df6, on=['quotation_num'], how='right')
df_result1

# %%
df_result1 = df_result1.rename(columns={'car_sk': 'car_id'})
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
# datetime_cols = df_result1.select_dtypes(include=["datetime", "datetimetz"]).columns

# for col in datetime_cols:
#     df_result1[col] = df_result1[col].astype(str).replace("NaT", None)


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
            # ตรวจสอบว่ามี quotation_num และ car_id หรือไม่
            if 'quotation_num' not in record or pd.isna(record['quotation_num']):
                print(f"⚠️ Skip row: no quotation_num: {record}")
                continue
            if 'car_id' not in record or pd.isna(record['car_id']):
                print(f"⚠️ Skip row: no car_id: {record}")
                continue

            # ✅ Update เฉพาะคอลัมน์เดียว
            stmt = (
                update(table)
                .where(table.c.quotation_num == record['quotation_num'])
                .values(car_id=record['car_id'])
            )
            conn.execute(stmt)

print("✅ Update car_id completed successfully.")

# %%
# df_result1.to_excel('test.xlsx', index=False, engine='openpyxl')

# %%



