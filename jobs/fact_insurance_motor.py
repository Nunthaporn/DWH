from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ✅ Load .env
load_dotenv()

# ✅ Source DB connections
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)
task_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task"
)

# ✅ Target PostgreSQL
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_motor_data():
    df_plan = pd.read_sql("""
        SELECT quo_num, company, company_prb, assured_insurance_capital1, is_addon, type, repair_type
        FROM fin_system_select_plan
        WHERE datestart >= '2025-01-01' AND datestart < '2025-07-01' AND type_insure = 'ประกันรถ'
    """, source_engine)

    df_order = pd.read_sql("""
        SELECT quo_num, responsibility1, responsibility2, responsibility3, responsibility4,
               damage1, damage2, damage3, damage4, protect1, protect2, protect3, protect4,
               IF(sendtype = 'ที่อยู่ใหม่', provincenew, province) AS delivery_province,
               show_ems_price, show_ems_type
        FROM fin_order
        WHERE datekey >= '2025-01-01' AND datekey < '2025-07-01'
    """, task_engine)

    df_pay = pd.read_sql("""
        SELECT quo_num, date_warranty, date_exp
        FROM fin_system_pay
        WHERE datestart >= '2025-01-01' AND datestart < '2025-07-01'
    """, source_engine)

    print("📦 df_plan:", df_plan.shape)
    print("📦 df_order:", df_order.shape)
    print("📦 df_pay:", df_pay.shape)

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
        return f"ชั้น{a_str} {b_str}"

    df["insurance_class"] = df.apply(lambda row: combine_columns(row["type"], row["repair_type"]), axis=1)
    df = df.drop(columns=["type", "repair_type"], errors="ignore")

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
        "date_exp": "date_expired"
    })

    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df_temp = df.replace(r'^\s*$', np.nan, regex=True)
    df["non_empty_count"] = df_temp.notnull().sum(axis=1)
    df = df.sort_values("non_empty_count", ascending=False).drop_duplicates(subset="quotation_num")
    df = df.drop(columns=["non_empty_count"], errors="ignore")

    df.columns = df.columns.str.lower()
    df["income_comp_ins"] = df["income_comp_ins"].apply(lambda x: True if x == 1 else False if x == 0 else None)
    df["insurance_class"] = df["insurance_class"].replace("ซ่อมอู่", np.nan)
    df["date_warranty"] = pd.to_datetime(df["date_warranty"], errors="coerce")
    df["date_expired"] = pd.to_datetime(df["date_expired"], errors="coerce")
    
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

    numeric_columns = [
        "sum_insured", "human_coverage_person", "human_coverage_atime", "property_coverage",
        "deductible", "vehicle_damage", "deductible_amount", "vehicle_theft_fire",
        "vehicle_flood_damage", "personal_accident_driver", "personal_accident_passengers",
        "medical_coverage", "driver_coverage", "ems_amount"
    ]

    for col in numeric_columns:
        if col in df.columns:
            # ทำความสะอาดข้อมูลก่อนแปลงเป็นตัวเลข
            df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
            # กรองเฉพาะค่าที่เป็นตัวเลขหรือค่าว่าง
            df[col] = df[col].apply(lambda x: x if x in ["", "None", "nan", "NaN"] or 
                                   (x.replace(".", "").replace("-", "").isdigit() and x != "ซ่อมอู่") 
                                   else None)
            # แปลงเป็นตัวเลขและจัดการกับ NaN values
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # ใช้ float64 แทน Int64 เพื่อหลีกเลี่ยงปัญหา casting
            df[col] = df[col].astype("float64")

    df = df.where(pd.notnull(df), None)

    print("\n📊 Cleaning completed")

    return df

@op
def load_motor_data(df: pd.DataFrame):
    table_name = "fact_insurance_motor"
    pk_column = "quotation_num"

    df = df[df[pk_column].notna()].copy()
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=target_engine)

    with target_engine.begin() as conn:
        df_existing = pd.read_sql(f"SELECT {pk_column} FROM {table_name}", conn)

    existing_ids = set(df_existing[pk_column])
    new_rows = df[~df[pk_column].isin(existing_ids)].copy()
    update_rows = df[df[pk_column].isin(existing_ids)].copy()

    print(f"🆕 Insert: {len(new_rows)} rows")
    print(f"🔄 Update: {len(update_rows)} rows")

    if not new_rows.empty:
        with target_engine.begin() as conn:
            conn.execute(table.insert(), new_rows.to_dict(orient="records"))

    if not update_rows.empty:
        with target_engine.begin() as conn:
            for record in update_rows.to_dict(orient="records"):
                stmt = pg_insert(table).values(**record)
                update_dict = {
                    c.name: stmt.excluded[c.name]
                    for c in table.columns if c.name != pk_column
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_dict
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")

@job
def fact_insurance_motor_etl():
    load_motor_data(clean_motor_data(extract_motor_data()))

if __name__ == "__main__":
    df_raw = extract_motor_data()

    df_clean = clean_motor_data((df_raw))
    print("✅ Cleaned columns:", df_clean.columns)

    # output_path = "fact_insurance_motor.csv"
    # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
    # print(f"💾 Saved to {output_path}")

    # output_path = "fact_insurance_motor.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_motor_data(df_clean)
    print("🎉 completed! Data upserted to fact_insurance_motor.")