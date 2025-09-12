from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import create_engine, MetaData, Table, inspect, text, func
import datetime
import time
from pymysql.cursors import SSCursor
import logging

# ✅ ตั้งค่า logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Load environment variables
load_dotenv()

# ===== TUNE ENGINES (เพิ่ม pre_ping + server-side cursor) =====
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    pool_pre_ping=True,  # ✅ ป้องกัน stale connection
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 600,
        'write_timeout': 600,
        'cursorclass': SSCursor,  # ✅ server-side cursor (streaming)
    }
)
source_engine_task = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    pool_pre_ping=True,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 600,
        'write_timeout': 600,
        'cursorclass': SSCursor,
    }
)

target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={
        'connect_timeout': 60,
        'options': '-c statement_timeout=300000'  # 5 minutes timeout
    }
)

# ===== Helper: read_sql with retry & streaming chunks =====
def read_sql_stream_with_retry(sql_text: str, engine, params=None, chunksize: int = 100_000, retries: int = 3, sleep_sec: int = 3):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with engine.connect().execution_options(stream_results=True) as conn:
                try:
                    conn.exec_driver_sql("SET SESSION net_read_timeout=600")
                    conn.exec_driver_sql("SET SESSION net_write_timeout=600")
                    conn.exec_driver_sql("SET SESSION wait_timeout=600")
                except Exception:
                    pass

                chunks = []
                for ch in pd.read_sql(text(sql_text), conn, params=params or {}, chunksize=chunksize):
                    chunks.append(ch)
                return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        except Exception as e:
            last_err = e
            logger.warning(f"⏳ read_sql attempt {attempt}/{retries} failed: {e}")
            time.sleep(sleep_sec * attempt)
    raise last_err

# ===== ใช้ใน extract(): ดึง plan แล้ว JOIN order เฉพาะล่าสุดต่อ quo_num =====
@op
def extract_sales_quotation_data():
    try:
        logger.info("📦 เริ่มดึงข้อมูลจาก source databases...")

        where_plan = """
            WHERE datestart BETWEEN '2025-01-01' AND '2025-09-07'
              AND id_cus NOT LIKE '%%FIN-TestApp%%'
              AND id_cus NOT LIKE '%%FIN-TestApp3%%'
              AND id_cus NOT LIKE '%%FIN-TestApp2%%'
              AND id_cus NOT LIKE '%%FIN-TestApp-2025%%'
              AND id_cus NOT LIKE '%%FIN-Tester1%%'
              AND id_cus NOT LIKE '%%FIN-Tester2%%'
              AND id_cus NOT LIKE '%%FTR22-9999%%'
              AND id_cus NOT LIKE '%%FIN19090009%%'
              AND id_cus NOT LIKE '%%fintest-01%%'
              AND id_cus NOT LIKE '%%fpc25-9999%%'
              AND id_cus NOT LIKE '%%bkk1_Hutsabodin%%'
              AND id_cus NOT LIKE '%%bkk1_Siraprapa%%'
              AND id_cus NOT LIKE '%%FNG22-072450%%'
              AND id_cus NOT LIKE '%%FNG23-087046%%'
              AND id_cus NOT LIKE '%%upc2_Siraprapa%%'
              AND id_cus NOT LIKE '%%THAI25-12345%%'
              AND id_cus NOT LIKE '%%FQ2408-24075%%'
              AND id_cus NOT LIKE '%%B2C-000000%%'
              AND id_cus NOT LIKE '%%123123%%'
              AND name NOT LIKE '%%ทดสอบ%%'
              AND name NOT LIKE '%%tes%%'
              AND name NOT LIKE '%%test%%'
              AND name NOT LIKE '%%เทสระบบ%%'
              AND name NOT LIKE '%%Tes ระบบ%%'
              AND name NOT LIKE '%%ทด่ท%%'
              AND name NOT LIKE '%%ทด สอบ%%'
              AND name NOT LIKE '%%ปัญญวัฒน์ โพธิ์ศรีทอง%%'
              AND name NOT LIKE '%%เอกศิษฎ์ เจริญธันยบูรณ์%%'
              AND name NOT LIKE '%%ทดสอย%%'
              AND name NOT LIKE '%%ทดสิบ%%'
              AND name NOT LIKE '%%ทดสอล%%'
              AND name NOT LIKE '%%ทด%%'
              AND name NOT LIKE '%%ทดมแ%%'
              AND name NOT LIKE '%%ทดดสอบ%%'
              AND name NOT LIKE '%%ทดลอง%%'
              AND name NOT LIKE '%%ทดลอง ทิพย%%'
              AND name NOT LIKE '%%ทดลองคีย์งาน%%'
              AND name NOT LIKE '%%ทดวสอบ%%'
              AND name NOT LIKE '%%ทอสอบ%%'
              AND name NOT LIKE '%%ทเสอบ%%'
              AND name NOT LIKE '%%ทบสอบ%%'
              AND lastname NOT LIKE '%%ทดสด%%'
              AND lastname NOT LIKE '%%ทดสอบ%%'
              AND lastname NOT LIKE '%%ทดสอบ2%%'
              AND lastname NOT LIKE '%%ทดสอบบบ%%'
              AND lastname NOT LIKE '%%ททดสอบสอน%%'
              AND lastname NOT LIKE '%%โวยวาย ทดสอบ%%'
              AND lastname NOT LIKE '%%โวยวายทดสอบ%%'
              AND lastname NOT LIKE '%%test%%'
              AND COALESCE(company, '') NOT LIKE '%%Testing%%'
        """
        sql_plan = f"""
            SELECT quo_num, type_insure, datestart, id_government_officer, status_gpf, quo_num_old,
                   status AS status_fssp, type_car, chanel_key, id_cus, name, lastname, company, fin_new_group,
                   isGovernmentOfficer, is_special_campaign, planType, current_campaign
            FROM fin_system_select_plan
            {where_plan}
        """
        df_plan = read_sql_stream_with_retry(sql_plan, source_engine)

        # ✅ ORDER: เลือกแถวล่าสุดต่อใบเสนอราคา ด้วย ROW_NUMBER()
        sql_order = f"""
            WITH latest_order AS (
            SELECT 
                o.quo_num,
                o.order_number,
                o.chanel,
                o.datekey,
                o.status AS status_fo,
                o.beaprakan   AS show_price_ins,
                o.prb         AS show_price_prb,
                o.totalprice  AS show_price_total,
                o.newinsurance AS newinsurance,

                CASE
                WHEN COALESCE(TRIM(o.status_paybill), '') = 'จ่ายปกติ'
                THEN 'เติมเงิน'
                ELSE 'ไม่เติมเงิน'
                END AS is_paybill,

                CASE
                WHEN o.viriyha LIKE '%ดวงเจริญ%'
                    AND o.newinsurance IN ('แอกซ่าประกันภัย','ฟอลคอนประกันภัย','เออร์โกประกันภัย','บริษัทกลาง')
                THEN 'ส่งผ่าน'
                ELSE 'ไม่ส่งผ่าน'
                END AS is_duangcharoen,

                CONCAT(IFNULL(o.title,''),' ',IFNULL(o.firstname,''),' ',IFNULL(o.lastname,'')) AS customer_name,
                o.tel,

                ROW_NUMBER() OVER (
                PARTITION BY o.quo_num
                ORDER BY o.datekey DESC, o.order_number DESC
                ) AS rn
            FROM fin_order o
            INNER JOIN (
                SELECT quo_num
                FROM fininsurance.fin_system_select_plan
                {where_plan}
            ) p ON p.quo_num = o.quo_num
            )
            SELECT
            quo_num,order_number,chanel,datekey,status_fo,show_price_ins,show_price_prb,
            show_price_total,is_paybill,is_duangcharoen,customer_name, tel, newinsurance 
            FROM latest_order
            WHERE rn = 1;
        """
        df_order = read_sql_stream_with_retry(sql_order, source_engine_task, chunksize=200_000)

        df_pay = read_sql_stream_with_retry("""
            SELECT quo_num, datestart, numpay, show_price_ins, show_price_prb, show_price_total,
                   show_price_check, show_price_service, show_price_taxcar, show_price_fine,
                   show_price_addon, show_price_payment, distax, show_ems_price, show_discount_ins,
                   discount_mkt, discount_government, discount_government_fin, discount_government_ins, 
                    coupon_addon, status AS status_fsp, status_detail, id_cus as id_cus_pay, clickbank
            FROM fin_system_pay
        """, source_engine, chunksize=200_000)

        df_risk = read_sql_stream_with_retry("""
            SELECT quo_num, type
            FROM fin_detail_plan_risk
            WHERE type = 'คอนโด'
        """, source_engine, chunksize=100_000)

        df_pa = read_sql_stream_with_retry("""
            SELECT quo_num, special_package
            FROM fin_detail_plan_pa
            WHERE special_package = 'CHILD'
        """, source_engine, chunksize=100_000)

        df_health = read_sql_stream_with_retry("""
            SELECT quo_num, special_package
            FROM fin_detail_plan_health
            WHERE special_package = 'CHILD'
        """, source_engine, chunksize=100_000)

        df_wp = read_sql_stream_with_retry("""
            SELECT cuscode as id_cus, display_permission
            FROM wp_users
            WHERE display_permission IN ('สำนักงานฟิน', 'หน้าร้านฟิน')
              AND name NOT LIKE '%%ทดสอบ%%'
              AND name NOT LIKE '%%tes%%'
              AND name NOT LIKE '%%test%%'
              AND name NOT LIKE '%%เทสระบบ%%'
              AND name NOT LIKE '%%Tes ระบบ%%'
              AND name NOT LIKE '%%ทด่ท%%'
              AND name NOT LIKE '%%ทด สอบ%%'
              AND name NOT LIKE '%%ปัญญวัฒน์ โพธิ์ศรีทอง%%'
              AND name NOT LIKE '%%เอกศิษฎ์ เจริญธันยบูรณ์%%'
              AND cuscode NOT LIKE '%%FIN-TestApp%%'
              AND cuscode NOT LIKE '%%FIN-Tester1%%'
              AND cuscode NOT LIKE '%%FIN-Tester2%%'
        """, source_engine, chunksize=100_000)

        # 🆕 NEW: ดึงรายชื่อ cuscode จาก fin_dna_log
        df_dna = read_sql_stream_with_retry("""
            SELECT cuscode
            FROM fininsurance.fin_dna_log
            WHERE cuscode IS NOT NULL AND cuscode <> ''
        """, source_engine, chunksize=100_000)

        df_flag = read_sql_stream_with_retry("""
            SELECT 
                quo_num,
                CASE 
                    WHEN status_big_agent = '1' THEN 'Big Agent'
                    ELSE NULL
                END AS big_agent
            FROM fininsurance.fin_quo_num_flag
            WHERE status_big_agent
        """, source_engine, chunksize=100_000)

        logger.info(
            f"📦 Shapes: plan={df_plan.shape}, order={df_order.shape}, pay={df_pay.shape}, "
            f"risk={df_risk.shape}, pa={df_pa.shape}, health={df_health.shape}, wp={df_wp.shape}, dna={df_dna.shape}, flag={df_flag.shape}"
        )

        # 🆕 NEW: ส่ง df_dna ออกไปด้วย
        return df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp, df_dna, df_flag

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการดึงข้อมูล: {e}")
        raise

@op
def clean_sales_quotation_data(inputs):
    try:
        # 🆕 NEW: รับ df_dna เพิ่มมา
        df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp, df_dna, df_flag = inputs
        logger.info("🧹 เริ่มทำความสะอาดข้อมูล...")

        # ========= merge เดิม =========
        df_merged = df_plan.merge(df_order, on='quo_num', how='left')
        df_merged = df_merged.merge(df_pay, on='quo_num', how='left', suffixes=('', '_pay'))
        df_merged = df_merged.merge(df_risk, on='quo_num', how='left', suffixes=('', '_risk'))
        df_merged = df_merged.merge(df_pa, on='quo_num', how='left', suffixes=('', '_pa'))
        df_merged = df_merged.merge(df_health, on='quo_num', how='left', suffixes=('', '_health'))
        df_merged = df_merged.merge(df_wp, on='id_cus', how='left')
        df_merged = df_merged.merge(df_flag, on='quo_num', how='left')

        # 🆕 NEW: merge กับ DNA (id_cus จากฝั่งงาน เทียบกับ cuscode ใน dna_log)
        df_merged = df_merged.merge(
            df_dna, left_on='id_cus', right_on='cuscode', how='left'
        )

        # 🆕 NEW: คำนวณ dna_fin ตาม COALESCE(CASE WHEN dna.cuscode IS NOT NULL THEN 'ตัวแทน DNA' END,'ตัวแทนทั่วไป')
        df_merged['dna_fin'] = np.where(
            df_merged['cuscode'].notna(), 'ตัวแทน DNA', 'ตัวแทนทั่วไป'
        )

        df_merged['customer_name'] = df_merged['customer_name'].replace(
            to_replace=r'.*- สหกรณ์อิสลาม อิบนูอัฟฟาน จำกัด$',
            value='สหกรณ์อิสลาม อิบนูอัฟฟาน จำกัด',
            regex=True
        )

        # ===== DEBUG + ROBUST NORMALIZATION FOR goverment_type_text =====

        # 0) ยืนยันคอลัมน์สำคัญมีอยู่จริง
        required_cols = [
            'isGovernmentOfficer', 'status_gpf', 'is_special_campaign',
            'current_campaign', 'newinsurance'
        ]
        for col in required_cols:
            if col not in df_merged.columns:
                logger.warning(f"⚠️ missing column: {col}")

        # ใช้ id_cus จาก pay ก่อน (sp.id_cus) ถ้าไม่มี fallback เป็นของ plan
        if 'id_cus_pay' in df_merged.columns:
            idcus_series = df_merged['id_cus_pay']
        else:
            idcus_series = df_merged.get('id_cus')
            logger.warning("⚠️ id_cus_pay not found; falling back to id_cus from plan")

        # 1) ฟังก์ชัน normalize ที่ทนทาน
        def to_str(s):
            return s.astype(str).str.strip()

        def norm_lower(s):
            return to_str(s).str.lower()

        # true-ish: รองรับหลายรูปแบบ + ตัวเลขทุกค่าที่ > 0 จะถือเป็น True
        def to_bool(s: pd.Series) -> pd.Series:
            s_str = norm_lower(s)
            truthy = {'1','y','yes','true','t','ใช่','true-ish','ok'}
            falsy  = {'0','n','no','false','f','ไม่','none','null','na',''}
            # ลองตีความตัวเลข > 0 เป็น True
            as_num = pd.to_numeric(s_str, errors='coerce')
            num_truth = as_num.fillna(0) > 0
            cat_truth = s_str.isin(truthy)
            cat_false = s_str.isin(falsy)
            # True ถ้าเป็น truthy หรือเป็นตัวเลข > 0
            # False ถ้าเป็น falsy ชัดเจน
            # ที่เหลือใช้ num_truth เป็นหลัก
            out = num_truth | cat_truth
            out = out & (~cat_false)
            return out.fillna(False)

        # 2) เตรียมซีรีส์
        is_go_raw = df_merged.get('isGovernmentOfficer', '')
        gpf_raw   = df_merged.get('status_gpf', '')
        is_sp_raw = df_merged.get('is_special_campaign', '')
        camp_raw  = df_merged.get('current_campaign', '')
        newin_raw = df_merged.get('newinsurance', '')
        idcus_raw = idcus_series if idcus_series is not None else pd.Series('', index=df_merged.index)

        is_go_f = to_bool(is_go_raw)
        is_sp_f = to_bool(is_sp_raw)

        gpf_l   = norm_lower(gpf_raw)
        # นับว่าเป็น "yes" ถ้าเข้ากลุ่มเหล่านี้ (เพิ่มคำไทย/รูปแบบอื่น ๆ ที่พบ)
        gpf_yes = gpf_l.isin({'yes','y','true','t','1','ใช่','gpf','กบข'})
        # (กันเคสอื่น ๆ ที่เป็นบวกด้วย)
        gpf_yes = gpf_yes | (pd.to_numeric(gpf_l, errors='coerce').fillna(0) > 0)

        camp_l  = norm_lower(camp_raw)
        newin_s = to_str(newin_raw)
        idcus_s = to_str(idcus_raw)

        # ธนชาตประกันภัย: ใช้ contains แบบหยวน (กันเว้นวรรค/ตัวสะกด)
        # ถ้าในระบบมีสะกดอื่น ๆ เพิ่มเติม เช่น "ธนชาต ประกันภัย", "ธนชาต-ประกันภัย" ให้เติม pattern ตรงนี้ได้
        is_tnc = newin_s.str.contains('ธนชาต', na=False)

        is_lady = camp_l.eq('lady')
        is_fng  = idcus_s.str.startswith('FNG', na=False)

        # 3) สร้าง mask ตามกติกา
        m1 = is_go_f & gpf_yes & (~is_sp_f)        # 'กบข'
        m2 = is_go_f & (~gpf_yes) & (~is_sp_f)     # 'กบข(ฟิน)'
        m3 = is_sp_f & is_tnc & is_fng             # 'customize_tnc'
        m4 = is_tnc & is_lady                      # 'lady'
        m5 = is_sp_f                                # 'สู้สุดใจ'

        # 4) สร้างคอลัมน์ผลลัพธ์
        df_merged['goverment_type_text'] = ''
        df_merged.loc[m1, 'goverment_type_text'] = 'กบข'
        df_merged.loc[m2, 'goverment_type_text'] = 'กบข(ฟิน)'
        df_merged.loc[m3, 'goverment_type_text'] = 'customize_tnc'
        df_merged.loc[m4, 'goverment_type_text'] = 'lady'
        df_merged.loc[m5 & (df_merged['goverment_type_text'] == ''), 'goverment_type_text'] = 'สู้สุดใจ'

        # 5) DEBUG: เช็คจำนวนที่เข้าเงื่อนไข + ตัวอย่างแถว
        print("m1 กบข", m1.sum(), "rows")
        print("m2 กบข(ฟิน)", m2.sum(), "rows")
        print("m3 customize_tnc", m3.sum(), "rows")
        print("m4 lady", m4.sum(), "rows")
        print("m5 สู้สุดใจ", m5.sum(), "rows")

        # ถ้าเงื่อนไขไหนยัง 0 ให้พิมพ์ distribution ของค่าที่เกี่ยว
        if m1.sum() == 0 or m2.sum() == 0 or m5.sum() == 0:
            print("📊 isGovernmentOfficer (top):")
            print(to_str(is_go_raw).value_counts(dropna=False).head(10))
            print("📊 status_gpf (top):")
            print(to_str(gpf_raw).value_counts(dropna=False).head(10))
            print("📊 is_special_campaign (top):")
            print(to_str(is_sp_raw).value_counts(dropna=False).head(10))

        if m3.sum() == 0 or m4.sum() == 0:
            print("📊 newinsurance (top):")
            print(newin_s.value_counts(dropna=False).head(10))
            print("📊 current_campaign (top):")
            print(to_str(camp_raw).value_counts(dropna=False).head(10))
            if 'id_cus_pay' in df_merged.columns:
                print("📊 id_cus_pay prefix (top):")
                print(df_merged['id_cus_pay'].astype(str).str[:3].value_counts(dropna=False).head(10))

        # แสดงตัวอย่าง 5 แถวที่ "เกือบ" เข้า (ช่วยไล่ดูค่าจริง)
        near_tnc = df_merged[newin_s.str.contains('ธนชาต', na=False)].head(5)
        print("🔎 sample rows with newinsurance contains 'ธนชาต':\n", near_tnc[['quo_num','newinsurance','current_campaign','is_special_campaign','id_cus','id_cus_pay']].head(5))


        # ===== (2) ล้างคอมม่า/ช่องว่างก่อนแปลงตัวเลข (เฉพาะคอลัมน์เงิน) =====
        money_cols = [
            'show_price_ins','show_price_prb','show_price_total',
            'show_price_check','show_price_service','show_price_taxcar','show_price_fine',
            'show_price_addon','show_price_payment','distax','show_ems_price','show_discount_ins',
            'discount_mkt','discount_government','discount_government_fin','discount_government_ins',
            'coupon_addon'
        ]
        for c in money_cols:
            if c in df_merged.columns:
                df_merged[c] = (
                    df_merged[c]
                    .astype(str)
                    .str.replace(',', '', regex=False)
                    .str.strip()
                    .replace({'': np.nan, 'nan': np.nan, 'None': np.nan, 'null': np.nan})
                )

        # override จาก order ก่อน pay
        override_pairs = [
            ("show_price_ins","show_price_ins_pay"),
            ("show_price_prb", "show_price_prb_pay"),
            ("show_price_total", "show_price_total_pay"),
        ]
        for base_col, pay_col in override_pairs:
            base_exists = base_col in df_merged.columns
            pay_exists = pay_col in df_merged.columns
            if base_exists and pay_exists:
                df_merged[base_col] = pd.to_numeric(df_merged[base_col], errors='coerce').combine_first(
                    pd.to_numeric(df_merged[pay_col], errors='coerce')
                )
                df_merged.drop(columns=[pay_col], inplace=True)
            elif (not base_exists) and pay_exists:
                df_merged[base_col] = pd.to_numeric(df_merged[pay_col], errors='coerce')
                df_merged.drop(columns=[pay_col], inplace=True)

        if 'tel' in df_merged.columns:
            df_merged['tel'] = (
                df_merged['tel']
                .astype(str)              # แปลงเป็น string
                .str.strip()              # ตัด space หน้า-หลัง
                .str.replace(r'\D+', '', regex=True)  # ลบทุกตัวที่ไม่ใช่ตัวเลข
                .replace({'': None})      # ถ้าไม่มีตัวเลขเลย -> None
            )

        # ทำความสะอาดทั่วไป
        df_merged = df_merged.replace(['nan', 'NaN', 'null', ''], np.nan)
        df_merged = df_merged.replace(r'^\s*$', np.nan, regex=True)
        df_merged = df_merged.where(pd.notnull(df_merged), None)

        # rename
        column_mapping = {
            "quo_num": "quotation_num",
            "datestart": "quotation_date",
            "datestart_pay": "transaction_date",
            "datekey": "order_time",
            "type_insure": "type_insurance",
            "id_government_officer": "rights_government",
            "status_gpf": "goverment_type",
            "quo_num_old": "quotation_num_old",
            "numpay": "installment_number",
            "show_price_ins": "ins_amount",
            "show_price_prb": "prb_amount",
            "show_price_total": "total_amount",
            "show_price_check": "show_price_check",
            "show_price_service": "service_price",
            "show_price_taxcar": "tax_car_price",
            "show_price_fine": "overdue_fine_price",
            "show_price_addon": "price_addon",
            "show_price_payment": "payment_amount",
            "distax": "tax_amount",
            "show_ems_price": "ems_amount",
            "show_discount_ins": "ins_discount",
            "discount_mkt": "mkt_discount",
            "discount_government": "goverment_discount",
            "discount_government_fin": "fin_goverment_discount",
            "discount_government_ins": "ins_goverment_discount",
            "coupon_addon": "discount_addon",
            "chanel": "contact_channel",
            "isGovernmentOfficer": "is_government_officer",
            "planType": "plan_type"
        }
        df_merged.rename(columns=column_mapping, inplace=True)

        df_merged['local_broker'] = np.where(
            df_merged['chanel_key'].astype(str).str.strip().eq('WEB-LOCAL'),
            'Local Broker',
            ''
        )

        # ===== (4) กันพลาดกรณี merge แล้วมีหลายบรรทัด (ควรเหลือน้อยจาก SQL แล้ว) =====
        # จัดลำดับให้บรรทัดที่ "ตัวเลขครบ" และ "order_time ล่าสุด" มาก่อน จากนั้นค่อย drop_duplicates
        amt_cols_after = ['ins_amount','prb_amount','total_amount','service_price','tax_car_price',
                          'overdue_fine_price','price_addon','payment_amount','tax_amount','ems_amount']
        for c in amt_cols_after:
            if c not in df_merged.columns:
                df_merged[c] = np.nan
        df_merged['_nonnull_amt'] = pd.DataFrame({c: pd.to_numeric(df_merged[c], errors='coerce') for c in amt_cols_after}).notna().sum(axis=1)

        sort_key = pd.to_datetime(df_merged.get('order_time'), errors='coerce')
        df_merged['_sort_key'] = sort_key
        df_merged.sort_values(by=['quotation_num','_nonnull_amt','_sort_key'], ascending=[True, False, False], inplace=True)
        df_merged = df_merged.drop_duplicates(subset=['quotation_num'], keep='first').drop(columns=['_nonnull_amt','_sort_key'])

        # sale_team
        def assign_sale_team(row):
            id_cus = str(row.get('id_cus') or '')
            type_insurance_raw = row.get('type_insurance')
            type_car_raw = row.get('type_car')

            type_insurance = str(type_insurance_raw).strip().lower()
            type_car = str(type_car_raw).strip().lower()
            chanel_key = str(row.get('chanel_key')).strip()
            special_package = str(row.get('special_package')).strip().upper()
            special_package_health = str(row.get('special_package_health')).strip().upper()
            fin_new_group = str(row.get('fin_new_group')).strip().upper()

            if chanel_key == 'WEB-SUBBROKER' or fin_new_group == 'FIN-BROKER':
                return 'Subbroker'
            if id_cus.startswith('FTR'):
                return 'Telesales'
            if type_car == 'fleet':
                return 'fleet'
            if type_car == 'ตะกาฟุล':
                return 'ตะกาฟุล'
            if type_insurance == 'ประกันรถ':
                return 'Motor agency'
            if type_insurance == 'ตรอ':
                return 'ตรอ'
            if chanel_key == 'CHILD':
                return 'ประกันเด็ก'
            if chanel_key in ['หน้าร้าน', 'หน้าร้านฟิน', 'สำนักงานฟิน']:
                return 'หน้าร้าน'
            if chanel_key == 'ตะกาฟุล':
                return 'ตะกาฟุล'
            if str(row.get('type')).strip() == 'คอนโด':
                return 'ประกันคอนโด'
            if special_package == 'CHILD' or special_package_health == 'CHILD':
                return 'ประกันเด็ก'
            if row.get('display_permission') in ['หน้าร้านฟิน', 'สำนักงานฟิน']:
                return 'หน้าร้าน'
            if pd.isna(type_insurance_raw) or type_insurance in ['', 'none', 'nan']:
                if pd.isna(type_car_raw) or type_car in ['', 'none', 'nan']:
                    return 'N/A'
            return 'Non Motor'

        df_merged['sale_team'] = df_merged.apply(assign_sale_team, axis=1)

        cols_to_drop = [
            'id_cus','type_car','chanel_key','special_package','special_package_health','type',
            'display_permission', 'name', 'lastname', 'company', 'type_insurance', 'fin_new_group', 
            'current_campaign', 'newinsurance','cuscode', 'id_cus_pay'
        ]
        df_merged.drop(columns=[c for c in cols_to_drop if c in df_merged.columns], inplace=True)

        # ✅ แปลงวันที่ (เก็บเป็น yyyymmdd string แล้วค่อย cast Int64 ได้)
        date_columns = ['transaction_date', 'order_time', 'quotation_date']
        for col in date_columns:
            if col in df_merged.columns:
                df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce').dt.strftime('%Y%m%d')

        # ✅ installment_number map
        if 'installment_number' in df_merged.columns:
            installment_mapping = {'0': '1', '03': '3', '06': '6', '08': '8'}
            df_merged['installment_number'] = df_merged['installment_number'].replace(installment_mapping)

        # ✅ สร้าง status
        def create_status_mapping():
            return {
                ('wait', ''): '1',
                ('wait-key', ''): '1',
                ('sendpay', 'sendpay'): '2',
                ('sendpay', 'verify-wait'): '2',
                ('tran-succ', 'sendpay'): '2',
                ('tran-succ', 'verify-wait'): '2',
                ('cancel', '88'): 'cancel',
                ('delete', ''): 'delete',
                ('wait', 'sendpay'): '2',
                ('delete', 'sendpay'): 'delete',
                ('delete', 'wait'): 'delete',
                ('delete', 'wait-key'): 'delete',
                ('wait', 'wait'): '1',
                ('wait', 'wait-key'): '1',
                ('', 'wait'): '1',
                ('cancel', ''): 'cancel',
                ('cancel', 'cancel'): 'cancel',
                ('delete', 'delete'): 'delete',
                ('active', 'verify'): '6',
                ('active', 'success'): '8',
                ('active', ''): '8'
            }
        status_mapping = create_status_mapping()

        df_merged['status_key'] = df_merged.apply(
            lambda row: (
                str(row.get('status_fssp') or '').strip(),
                str(row.get('status_fsp') or '').strip()
            ), axis=1
        )
        df_merged['status'] = df_merged['status_key'].map(status_mapping)
        df_merged.loc[df_merged['status_key'].apply(lambda x: 'cancel' in x), 'status'] = 'cancel'
        df_merged.loc[df_merged['status_key'].apply(lambda x: 'delete' in x), 'status'] = 'delete'

        if 'status_fo' in df_merged.columns:
            fo_mask = df_merged['status_fo'].notna()
            df_merged.loc[fo_mask, 'status'] = df_merged.loc[fo_mask, 'status_fo'].apply(
                lambda x: 'cancel' if x == '88' else x
            )
            df_merged.drop(columns=['status_fo'], inplace=True)

        for c in ['status_fssp', 'status_fsp', 'status_key']:
            if c in df_merged.columns:
                df_merged.drop(columns=[c], inplace=True)

        # ✅ ลบข้อมูลซ้ำ (เหลือน้อยลงมากจากการจัด sort แล้ว)
        df_merged.drop_duplicates(subset=['quotation_num'], keep='first', inplace=True)

        # ✅ แปลงคอลัมน์ตัวเลข (เงินเป็น float; อย่าแคสต์เป็น Int64)
        numeric_columns = [
            'installment_number', 'show_price_check', 'ems_amount', 'service_price',
            'ins_amount', 'prb_amount', 'total_amount', 'tax_car_price', 'overdue_fine_price',
            'ins_discount', 'mkt_discount', 'payment_amount', 'price_addon', 'discount_addon',
            'goverment_discount', 'tax_amount', 'fin_goverment_discount', 'ins_goverment_discount'
        ]
        for col in numeric_columns:
            if col in df_merged.columns:
                df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')
                df_merged[col] = df_merged[col].replace([np.inf, -np.inf], None)

        # ✅ แคสต์ Int64 เฉพาะคอลัมน์ "วันที่/ตัวนับ" จริงๆ
        int8_cols = [
            'transaction_date', 'order_time', 'installment_number', 'show_price_check', 'quotation_date'
        ]
        for col in int8_cols:
            if col in df_merged.columns:
                df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce').astype('Int64')

        # if 'status_detail' in df_merged.columns:
        #     df_merged = df_merged[
        #         ~df_merged['status_detail'].astype(str).str.contains(r'(ทดสอบระบบ|เทสระบบ)', na=False)
        #     ]

        if 'status_detail' in df_merged.columns:
            before = len(df_merged)
            mask_test = df_merged['status_detail'].astype(str).str.contains('ทดสอบระบบ|เทสระบบ', regex=True, na=False)
            df_merged = df_merged.loc[~mask_test].copy()
            removed = before - len(df_merged)
            logger.info(f"🧪 ตัดแถว test (ทดสอบระบบ/เทสระบบ) ออก {removed:,} แถว")

        logger.info("✅ การทำความสะอาดข้อมูลเสร็จสิ้น")
        return df_merged

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการทำความสะอาดข้อมูล: {e}")
        raise

@op
def load_sales_quotation_data(df: pd.DataFrame):
    table_name = 'fact_sales_quotation'
    pk_column = 'quotation_num'

    df = df[~df[pk_column].duplicated(keep='first')].copy()
    df = df[df[pk_column].notna()].copy()
    if df.empty:
        print("⚠️ No valid data to process")
        return

    table = Table(table_name, MetaData(), autoload_with=target_engine)
    table_cols = [c.name for c in table.columns]

    valid_cols = [c for c in df.columns if c in table_cols]
    if pk_column not in valid_cols:
        raise ValueError(f"Primary key '{pk_column}' not present in dataframe columns: {valid_cols}")

    df = df[valid_cols].copy()

    print(f"📊 Upserting rows: {len(df)}")
    print(f"🔍 Columns used: {valid_cols}")

    def to_db_value(v):
        if pd.isna(v) or v is pd.NaT or v == '':
            return None
        return v

    now_ts = pd.Timestamp.now()
    records = []
    for _, row in df.iterrows():
        rec = {col: to_db_value(row.get(col)) for col in valid_cols}
        if 'create_at' in table_cols and 'create_at' not in rec:
            rec['create_at'] = now_ts
        if 'update_at' in table_cols and 'update_at' not in rec:
            rec['update_at'] = now_ts
        records.append(rec)

    insert_stmt = pg_insert(table)
    update_columns = {}
    for c in table.columns:
        col = c.name
        if col in [pk_column, 'create_at', 'update_at']:
            continue
        update_columns[col] = func.coalesce(insert_stmt.excluded[col], getattr(table.c, col))

    if 'update_at' in table_cols:
        update_columns['update_at'] = func.now()

    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[pk_column],
        set_=update_columns
    )

    with target_engine.begin() as conn:
        conn.execute(upsert_stmt, records)

    print("✅ Upsert (insert/update) completed.")

@job
def fact_sales_quotation_etl():
    data = extract_sales_quotation_data()
    df_clean = clean_sales_quotation_data(data)
    load_sales_quotation_data(df_clean)

if __name__ == "__main__":
    try:
        logger.info("🚀 เริ่มการประมวลผล fact_sales_quotation...")
        df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp, df_dna, df_flag = extract_sales_quotation_data()

        df_clean = clean_sales_quotation_data((df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp, df_dna, df_flag))

        output_path = "fact_sales_quotation.xlsx"
        df_clean.to_excel(output_path, index=False, engine='openpyxl')
        print(f"💾 Saved to {output_path}")

        # load_sales_quotation_data(df_clean)
        logger.info("🎉 completed! Data upserted to fact_sales_quotation.")
    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการประมวลผล: {e}")
        import traceback
        traceback.print_exc()
        raise
