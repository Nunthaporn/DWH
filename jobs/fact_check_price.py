from dagster import op, job
import pandas as pd
import numpy as np
import json
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, text, func, update, bindparam
from sqlalchemy.sql import tuple_
from datetime import datetime, timedelta
from sqlalchemy.dialects.postgresql import insert as pg_insert

# py>=3.9
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# =========================
# 🔧 ENV & CONNECTIONS
# =========================
load_dotenv()

# ✅ DB source (MariaDB)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_pre_ping=True, pool_recycle=3600
)

# ✅ DB target (PostgreSQL)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@"
    f"{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_pre_ping=True, pool_recycle=3600,
    connect_args={"connect_timeout": 30, "application_name": "fact_check_price_etl",
                  "options": "-c statement_timeout=300000"}
)

# =========================
# 🧰 Helpers
# =========================
THAI_DIGITS = str.maketrans("๐๑๒๓๔๕๖๗๘๙", "0123456789")

def normalize_money(sr: pd.Series) -> pd.Series:
    """
    แปลงสตริงจำนวนเงิน -> float (รองรับเลขไทย, คอมม่า, ช่วงค่า)
    """
    s = sr.astype(str).str.strip().str.translate(THAI_DIGITS)
    s = s.str.replace(r"[^\d\.\-,]", "", regex=True)   # เก็บเฉพาะ 0-9.-,
    s = s.str.replace(",", "", regex=False)            # ตัดคอมม่า
    s = s.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(s, errors="coerce").astype("Float64")

def _json_extract_names(results_json, limit=4):
    if pd.isna(results_json):
        return [None]*limit
    try:
        data = json.loads(results_json)
    except Exception:
        return [None]*limit
    names = []
    if isinstance(data, list):
        for d in data:
            if isinstance(d, dict):
                names.append(d.get("company_name"))
            if len(names) >= limit:
                break
    elif isinstance(data, dict):
        names.append(data.get("company_name"))
    names += [None]*(limit-len(names))
    return names[:limit]

def _json_extract_selected(selected_json):
    if pd.isna(selected_json):
        return None
    try:
        data = json.loads(selected_json)
    except Exception:
        return None
    if isinstance(data, list) and data:
        item0 = data[0]
        return item0.get("company_name") if isinstance(item0, dict) else None
    if isinstance(data, dict):
        return data.get("company_name")
    return None

def _extract_plate(text_val):
    if pd.isna(text_val) or str(text_val).strip() == "":
        return None
    text = str(text_val).strip()
    # ลบชื่อจังหวัดก่อนจับป้ายทะเบียน
    provinces = [
        "กรุงเทพมหานคร","กระบี่","กาญจนบุรี","กาฬสินธุ์","กำแพงเพชร","ขอนแก่น","จันทบุรี","ฉะเชิงเทรา","ชลบุรี",
        "ชัยนาท","ชัยภูมิ","ชุมพร","เชียงใหม่","เชียงราย","ตรัง","ตราด","ตาก","นครนายก","นครปฐม","นครพนม",
        "นครราชสีมา","นครศรีธรรมราช","นครสวรรค์","นนทบุรี","นราธิวาส","น่าน","บึงกาฬ","บุรีรัมย์","ปทุมธานี",
        "ประจวบคีรีขันธ์","ปราจีนบุรี","ปัตตานี","พระนครศรีอยุธยา","พังงา","พัทลุง","พิจิตร","พิษณุโลก",
        "เพชรบุรี","เพชรบูรณ์","แพร่","พะเยา","ภูเก็ต","มหาสารคาม","มุกดาหาร","แม่ฮ่องสอน","ยะลา","ยโสธร",
        "ระนอง","ระยอง","ราชบุรี","ร้อยเอ็ด","ลพบุรี","ลำปาง","ลำพูน","เลย","ศรีสะเกษ","สกลนคร","สงขลา",
        "สตูล","สมุทรปราการ","สมุทรสงคราม","สมุทรสาคร","สระแก้ว","สระบุรี","สิงห์บุรี","สุโขทัย","สุพรรณบุรี",
        "สุราษฎร์ธานี","สุรินทร์","หนองคาย","หนองบัวลำภู","อ่างทอง","อุดรธานี","อุทัยธานี","อุตรดิตถ์",
        "อุบลราชธานี","อำนาจเจริญ"
    ]
    for prov in provinces:
        text = text.replace(prov, "")
    if '//' in text:
        text = text.split('//')[0]
    elif '/' in text:
        text = text.split('/')[0]
    text = (text.replace('-', '')
                .replace('/', '')
                .replace(',', '')
                .replace('+', '')
                .replace(' ', ''))
    text = re.sub(r'^ป\d+และป\d+\+?', '', text)
    text = re.sub(r'^ป\d+\+?', '', text)
    text = re.sub(r'^งานป\d+', '', text)
    m = re.match(r'(\d{1}[ก-ฮ]{2}\d{1,4}|[ก-ฮ]{1,3}\d{1,4})', text)
    if not m:
        return None
    plate = m.group(1)
    return plate if len(plate) > 3 else None

def _extract_province(text_val):
    if pd.isna(text_val) or str(text_val).strip() == "":
        return None
    s = str(text_val)
    for prov in [
        "กรุงเทพมหานคร","กระบี่","กาญจนบุรี","กาฬสินธุ์","กำแพงเพชร","ขอนแก่น","จันทบุรี","ฉะเชิงเทรา","ชลบุรี",
        "ชัยนาท","ชัยภูมิ","ชุมพร","เชียงใหม่","เชียงราย","ตรัง","ตราด","ตาก","นครนายก","นครปฐม","นครพนม",
        "นครราชสีมา","นครศรีธรรมราช","นครสวรรค์","นนทบุรี","นราธิวาส","น่าน","บึงกาฬ","บุรีรัมย์","ปทุมธานี",
        "ประจวบคีรีขันธ์","ปราจีนบุรี","ปัตตานี","พระนครศรีอยุธยา","พังงา","พัทลุง","พิจิตร","พิษณุโลก",
        "เพชรบุรี","เพชรบูรณ์","แพร่","พะเยา","ภูเก็ต","มหาสารคาม","มุกดาหาร","แม่ฮ่องสอน","ยะลา","ยโสธร",
        "ระนอง","ระยอง","ราชบุรี","ร้อยเอ็ด","ลพบุรี","ลำปาง","ลำพูน","เลย","ศรีสะเกษ","สกลนคร","สงขลา",
        "สตูล","สมุทรปราการ","สมุทรสงคราม","สมุทรสาคร","สระแก้ว","สระบุรี","สิงห์บุรี","สุโขทัย","สุพรรณบุรี",
        "สุราษฎร์ธานี","สุรินทร์","หนองคาย","หนองบัวลำภู","อ่างทอง","อุดรธานี","อุทัยธานี","อุตรดิตถ์",
        "อุบลราชธานี","อำนาจเจริญ"
    ]:
        if prov in s:
            return prov
    return None

def _bkk_today_range():
    """คืนค่า start,end ของ 'วันนี้' (ตามเวลาไทย) แบบ naive (ไม่มี tz) สำหรับใช้ใน SQL"""
    if ZoneInfo:
        tz = ZoneInfo("Asia/Bangkok")
        now_th = datetime.now(tz)
        start_th = now_th.replace(hour=0, minute=0, second=0, microsecond=0)
        end_th = start_th + timedelta(days=1)
        return start_th.replace(tzinfo=None), end_th.replace(tzinfo=None)
    # fallback UTC+7
    now = datetime.utcnow() + timedelta(hours=7)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

# =========================
# 🧲 EXTRACT (วันนี้ - เวลาไทย)
# =========================
@op
def extract_check_price_data() -> pd.DataFrame:
    start, end = _bkk_today_range()

    # start = '2025-01-01 00:00:00'
    # end = '2025-10-31 25:59:59'

    q_logs = text("""
        SELECT cuscode, brand, series, subseries, year, no_car, type, repair_type,
               assured_insurance_capital1, camera, addon, quo_num, create_at,
               results, selected, carprovince
        FROM fin_customer_logs_B2B
        WHERE create_at >= :start AND create_at < :end
    """)
    q_check = text("""
        SELECT id_cus, datekey, brand, model, submodel, yearcar, idcar, nocar,
               type_ins, company, tunprakan, deduct, status, type_driver,
               type_camera, type_addon, status_send
        FROM fin_checkprice
        WHERE datekey >= :start AND datekey < :end
    """)
    with source_engine.begin() as conn:
        df_logs = pd.read_sql(q_logs, conn, params={"start": start, "end": end})
        df_check = pd.read_sql(q_check, conn, params={"start": start, "end": end})
    return pd.DataFrame({"logs": [df_logs], "check": [df_check]})

@op
def clean_check_price_data(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw['logs'][0].copy()
    df1 = raw['check'][0].copy()

    # --- parse "results" เป็น 4 บริษัท ---
    def extract_company_names(x):
        if pd.isnull(x):
            return [None, None, None, None]
        try:
            data = json.loads(x)
        except Exception:
            return [None, None, None, None]
        names = []
        if isinstance(data, list):
            for d in data:
                if isinstance(d, dict):
                    names.append(d.get('company_name'))
                if len(names) == 4:
                    break
        elif isinstance(data, dict):
            names.append(data.get('company_name'))
        while len(names) < 4:
            names.append(None)
        return names[:4]

    company_names_df = df['results'].apply(extract_company_names).apply(pd.Series)
    company_names_df.columns = ['company_name1', 'company_name2', 'company_name3', 'company_name4']
    df = pd.concat([df.drop(columns=['results']), company_names_df], axis=1)

    # --- parse "selected" เป็นบริษัทที่เลือก ---
    def extract_selected_name(x):
        if pd.isnull(x):
            return None
        try:
            data = json.loads(x)
        except Exception:
            return None
        if isinstance(data, list) and len(data) > 0:
            item0 = data[0]
            return item0.get('company_name') if isinstance(item0, dict) else None
        elif isinstance(data, dict):
            return data.get('company_name')
        else:
            return None

    df['selecteds'] = df['selected'].apply(extract_selected_name)
    df.drop(columns=['selected'], inplace=True)

    # --- map ค่า camera/addon ---
    df['camera'] = df['camera'].map({
        'yes': 'มีกล้องหน้ารถ',
        'no': 'ไม่มีกล้องหน้ารถ'
    })
    df['addon'] = df['addon'].map({
        'ไม่มี': 'ไม่มีการต่อเติม'
    })

    # --- rename คอลัมน์ให้ตรงเป้าหมาย ---
    df.rename(columns={
        'cuscode': 'id_cus',
        'series': 'model',
        'subseries': 'submodel',
        'year': 'yearcar',
        'no_car': 'car_code',
        'assured_insurance_capital1': 'sum_insured',
        'camera': 'type_camera',
        'addon': 'type_addon',
        'quo_num': 'select_quotation',
        'create_at': 'transaction_date',
        'carprovince': 'province_car'
    }, inplace=True)

    # --- type_insurance จาก type + repair_type ---
    df['type_insurance'] = 'ชั้น' + df['type'].astype(str) + df['repair_type'].astype(str)
    df.drop(columns=['type', 'repair_type'], inplace=True)
    df['input_type'] = 'auto'

    # --- แยกชื่อบริษัทจาก fin_checkprice.company ---
    def split_company_names(x):
        if pd.isnull(x):
            return [None, None, None, None]
        names = [name.strip() for name in str(x).split('/')]
        while len(names) < 4:
            names.append(None)
        return names[:4]

    company_names_df = df1['company'].apply(split_company_names).apply(pd.Series)
    company_names_df.columns = ['company_name1', 'company_name2', 'company_name3', 'company_name4']
    df1 = pd.concat([df1.drop(columns=['company']), company_names_df], axis=1)

    # --- หาเลขทะเบียนและจังหวัดจาก idcar ---
    province_list = [
        "กรุงเทพมหานคร", "กระบี่", "กาญจนบุรี", "กาฬสินธุ์", "กำแพงเพชร", "ขอนแก่น", "จันทบุรี", "ฉะเชิงเทรา",
        "ชลบุรี", "ชัยนาท", "ชัยภูมิ", "ชุมพร", "เชียงใหม่", "เชียงราย", "ตรัง", "ตราด", "ตาก", "นครนายก",
        "นครปฐม", "นครพนม", "นครราชสีมา", "นครศรีธรรมราช", "นครสวรรค์", "นนทบุรี", "นราธิวาส", "น่าน",
        "บึงกาฬ", "บุรีรัมย์", "ปทุมธานี", "ประจวบคีรีขันธ์", "ปราจีนบุรี", "ปัตตานี", "พระนครศรีอยุธยา",
        "พังงา", "พัทลุง", "พิจิตร", "พิษณุโลก", "เพชรบุรี", "เพชรบูรณ์", "แพร่", "พะเยา", "ภูเก็ต",
        "มหาสารคาม", "มุกดาหาร", "แม่ฮ่องสอน", "ยะลา", "ยโสธร", "ระนอง", "ระยอง", "ราชบุรี",
        "ร้อยเอ็ด", "ลพบุรี", "ลำปาง", "ลำพูน", "เลย", "ศรีสะเกษ", "สกลนคร", "สงขลา", "สตูล",
        "สมุทรปราการ", "สมุทรสงคราม", "สมุทรสาคร", "สระแก้ว", "สระบุรี", "สิงห์บุรี", "สุโขทัย",
        "สุพรรณบุรี", "สุราษฎร์ธานี", "สุรินทร์", "หนองคาย", "หนองบัวลำภู", "อ่างทอง", "อุดรธานี",
        "อุทัยธานี", "อุตรดิตถ์", "อุบลราชธานี", "อำนาจเจริญ"
    ]

    def extract_clean_plate(value):
        if pd.isnull(value) or str(value).strip() == "":
            return None
        text = str(value).strip()
        for prov in province_list:
            if prov in text:
                text = text.replace(prov, "").strip()
        if '//' in text:
            text = text.split('//')[0].strip()
        elif '/' in text:
            text = text.split('/')[0].strip()
        text_cleaned = text.replace('-', '').replace('/', '').replace(',', '').replace('+', '').replace(' ', '')
        text_cleaned = re.sub(r'^ป\d+และป\d+\+?', '', text_cleaned)
        text_cleaned = re.sub(r'^ป\d+\+?', '', text_cleaned)
        text_cleaned = re.sub(r'^งานป\d+', '', text_cleaned)
        pattern_plate = r'(\d{1}[ก-ฮ]{2}\d{1,4}|[ก-ฮ]{1,3}\d{1,4})'
        match_plate = re.match(pattern_plate, text_cleaned)
        if match_plate:
            plate = match_plate.group(1)
            if len(plate) <= 3:
                return None
            return plate
        return None

    def extract_province(value):
        if pd.isnull(value) or str(value).strip() == "":
            return None
        for prov in province_list:
            if prov in str(value):
                return prov
        return None

    df1['id_car'] = df1['idcar'].apply(extract_clean_plate)
    df1['province_car'] = df1['idcar'].apply(extract_province)
    df1.drop(columns=['idcar'], inplace=True)

    df1 = df1[~df1['id_cus'].isin(['ทศพรทดสอบ', 'ชัดเจนทดสอบ', 'FIN-TestApp'])]
    df1.rename(columns={
        'datekey': 'transaction_date',
        'nocar': 'car_code',
        'type_ins': 'type_insurance',
        'tunprakan': 'sum_insured',
        'deduct': 'deductible'
    }, inplace=True)
    df1['input_type'] = 'manual'

    # --- รวมสองแหล่ง ---
    df_combined = pd.concat([df, df1], ignore_index=True, sort=False)

    # --- ทำความสะอาดสตริงว่างให้เป็น NaN/None ---
    for col in df_combined.columns:
        if df_combined[col].dtype == 'object':
            df_combined[col] = df_combined[col].replace(r'^\s*$', np.nan, regex=True)
    df_combined = df_combined.where(pd.notnull(df_combined), None)

    # --- ตัดช่องว่างหัวท้ายของสตริง ---
    for col in df_combined.columns:
        if df_combined[col].dtype == 'object':
            df_combined[col] = df_combined[col].apply(lambda x: x.lstrip() if isinstance(x, str) else x)

    # --- ตัด test users ---
    df_combined = df_combined[~df_combined['id_cus'].isin([
        'FIN-TestApp', 'FIN-TestApp2', 'FIN-TestApp3',
        'FIN-TestApp6', 'FIN-TestApp-2025', 'FNGtest', '????♡☆umata☆??'
    ])]

    # --- แปลงเงิน: เอาลูกน้ำ/สัญลักษณ์ออกแล้วเป็นตัวเลข ---
    for c in ["sum_insured", "deductible"]:
        if c in df_combined.columns:
            df_combined[c] = normalize_money(df_combined[c])

    # --- แปลงวันที่แบบ vectorized เป็น YYYYMMDD (Int64) ---
    date_columns = ['transaction_date']
    for col in date_columns:
        if col in df_combined.columns:
            df_combined[col] = pd.to_datetime(df_combined[col], errors='coerce')
            df_combined[col] = df_combined[col].dt.strftime('%Y%m%d').astype('Int64')

    # --- types อื่น ๆ ---
    df_combined['yearcar'] = pd.to_numeric(df_combined['yearcar'], errors='coerce').astype('Int64')
    df_combined['id_cus'] = df_combined['id_cus'].astype(str)

    # --- แปลง NaN -> None อีกครั้งเพื่อความชัวร์ก่อนคืนค่า ---
    df_combined = df_combined.replace({np.nan: None})

    # --- ลบคอลัมน์ที่ไม่ต้องการก่อนส่งออก ---
    drop_cols = ['results', 'selected']
    for c in drop_cols:
        if c in df_combined.columns:
            df_combined.drop(columns=[c], inplace=True)

    # --- ตรวจสอบคอลัมน์สุดท้ายก่อนส่งออก ---
    print("🧾 Columns before load:", df_combined.columns.tolist())

    return df_combined

@op
def load_check_price_data(df: pd.DataFrame):
    try:
        table_name = 'fact_check_price'
        tbl = Table(table_name, MetaData(), autoload_with=target_engine)

        # ✅ ลบคอลัมน์ที่ไม่มีในตาราง
        valid_cols = set(tbl.columns.keys())
        df = df[[c for c in df.columns if c in valid_cols]]

        df = df.replace({np.nan: None})
        records = df.to_dict(orient='records')

        if not records:
            print("ℹ️ No records to insert.")
            return

        with target_engine.begin() as conn:
            stmt = tbl.insert().values(records)
            conn.execute(stmt)

        print(f"✅ Insert completed: {len(records)} rows inserted (duplicates allowed).")
    except Exception as e:
        print(f"❌ Error in load_check_price_data: {e}")
        raise

@job
def fact_check_price_etl():
    load_check_price_data(clean_check_price_data(extract_check_price_data()))

if __name__ == "__main__":
    df_raw = extract_check_price_data()
    # print("✅ Extracted logs:", df_raw.shape)

    df_clean = clean_check_price_data(df_raw)
#     print("✅ Cleaned columns:", df_clean.columns)

    # output_path = "fact_check_price.csv"
    # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
    # print(f"💾 Saved to {output_path}")

    # output_path = "fact_check_price1.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_check_price_data(df_clean)
    print("🎉 completed! Data upserted to fact_check_price.")
