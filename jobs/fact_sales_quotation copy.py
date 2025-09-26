from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import create_engine, MetaData, Table, inspect, text, func, or_
import datetime
import time
from pymysql.cursors import SSCursor
import logging

# ✅ ตั้งค่า logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Load environment variables
load_dotenv()

# ===== TIMEZONE helper (Asia/Bangkok) =====
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# ===== TUNE ENGINES (เพิ่ม pre_ping + server-side cursor) =====
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
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

        start_dt = '2025-09-20 00:00:00'
        end_dt   = '2025-09-30 00:00:00'  # ✅ ใช้ < end_dt (exclusive)

        logger.info(f"⏱️ Window (TH): {start_dt} → {end_dt}")

        where_plan = """
            WHERE datestart >= :start_dt AND datestart < :end_dt
                AND id_cus NOT IN ('FIN-TestApp','FIN-TestApp3','FIN-TestApp2','FIN-TestApp-2025',
                                'FIN-Tester1','FIN-Tester2','FTR22-9999','FIN19090009',
                                'fintest-01','fpc25-9999','bkk1_Hutsabodin','bkk1_Siraprapa',
                                'FNG22-072450','FNG23-087046','upc2_Siraprapa','THAI25-12345',
                                'FQ2408-24075','B2C-000000','123123')
                AND name NOT IN ('ทดสอบ','tes','test','เทสระบบ','Tes ระบบ','ทด่ท','ทด สอบ',
                                'ปัญญวัฒน์ โพธิ์ศรีทอง','เอกศิษฎ์ เจริญธันยบูรณ์','ทดสอย',
                                'ทดสิบ','ทดสอล','ทด','ทดมแ','ทดดสอบ','ทดลอง','ทดลอง ทิพย',
                                'ทดลองคีย์งาน','ทดวสอบ','ทอสอบ','ทเสอบ','ทบสอบ')
                AND lastname NOT IN ('ทดสด','ทดสอบ','ทดสอบ2','ทดสอบบบ','ททดสอบสอน',
                                    'โวยวาย ทดสอบ','โวยวายทดสอบ','test')
                AND COALESCE(company, '') NOT LIKE '%%Testing%%'
        """

        sql_plan = f"""
            SELECT quo_num, type_insure, datestart, id_government_officer, status_gpf, quo_num_old,
                   status AS status_fssp, type_car, chanel_key, id_cus, name, lastname, company, fin_new_group,
                   isGovernmentOfficer, is_special_campaign, planType, current_campaign
            FROM fin_system_select_plan
            {where_plan}
        """
        df_plan = read_sql_stream_with_retry(sql_plan, source_engine, params={"start_dt": start_dt, "end_dt": end_dt})

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
                CASE WHEN COALESCE(TRIM(o.status_paybill), '') = 'จ่ายปกติ' THEN 'เติมเงิน' ELSE 'ไม่เติมเงิน' END AS is_paybill,
                CASE
                    WHEN o.viriyha = 'ดวงเจริญ'
                         AND o.newinsurance IN ('แอกซ่าประกันภัย','ฟอลคอนประกันภัย','เออร์โกประกันภัย','บริษัทกลาง', 'เมืองไทยประกันภัย', 'ทิพยประกันภัย')
                    THEN 'ส่งผ่าน' ELSE 'ไม่ส่งผ่าน'
                END AS is_duangcharoen,
                CONCAT(IFNULL(o.title,''),' ',IFNULL(o.firstname,''),' ',IFNULL(o.lastname,'')) AS customer_name,
                o.tel,
                ROW_NUMBER() OVER (PARTITION BY o.quo_num ORDER BY o.datekey DESC, o.order_number DESC) AS rn
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
        df_order = read_sql_stream_with_retry(sql_order, source_engine_task, params={"start_dt": start_dt, "end_dt": end_dt}, chunksize=200_000)

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

        df_dna = read_sql_stream_with_retry("""
            SELECT cuscode
            FROM fininsurance.fin_dna_log
            WHERE cuscode IS NOT NULL AND cuscode <> ''
        """, source_engine, chunksize=100_000)

        df_flag = read_sql_stream_with_retry("""
            SELECT 
                quo_num,
                CASE WHEN status_big_agent = '1' THEN 'Big Agent' ELSE NULL END AS big_agent
            FROM fininsurance.fin_quo_num_flag
            WHERE status_big_agent
        """, source_engine, chunksize=100_000)

        logger.info(
            f"📦 Shapes: plan={df_plan.shape}, order={df_order.shape}, pay={df_pay.shape}, "
            f"risk={df_risk.shape}, pa={df_pa.shape}, health={df_health.shape}, wp={df_wp.shape}, dna={df_dna.shape}, flag={df_flag.shape}"
        )

        return df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp, df_dna, df_flag

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการดึงข้อมูล: {e}")
        raise

@op
def clean_sales_quotation_data(inputs):
    try:
        df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp, df_dna, df_flag = inputs
        logger.info("🧹 เริ่มทำความสะอาดข้อมูล...")

        # ========= merge =========
        df_merged = df_plan.merge(df_order, on='quo_num', how='left')
        df_merged = df_merged.merge(df_pay, on='quo_num', how='left', suffixes=('', '_pay'))
        df_merged = df_merged.merge(df_risk, on='quo_num', how='left', suffixes=('', '_risk'))
        df_merged = df_merged.merge(df_pa, on='quo_num', how='left', suffixes=('', '_pa'))
        df_merged = df_merged.merge(df_health, on='quo_num', how='left', suffixes=('', '_health'))
        df_merged = df_merged.merge(df_wp, on='id_cus', how='left')
        df_merged = df_merged.merge(df_flag, on='quo_num', how='left')
        df_merged = df_merged.merge(df_dna, left_on='id_cus', right_on='cuscode', how='left')

        df_merged['dna_fin'] = np.where(df_merged['cuscode'].notna(), 'ตัวแทน DNA', 'ตัวแทนทั่วไป')

        df_merged['customer_name'] = df_merged['customer_name'].replace(
            {
                r'.*- สหกรณ์อิสลาม อิบนูอัฟฟาน จำกัด$': 'สหกรณ์อิสลาม อิบนูอัฟฟาน จำกัด',
                r'.*- สหกรณ์ออมทรัพย์สุริยพงษ์ จำกัด$': 'สหกรณ์ออมทรัพย์สุริยพงษ์ จำกัด',
                r'.*- สหกรณ์อัล-อามีน สตูล จำกัด$': 'สหกรณ์อัล-อามีน สตูล จำกัด',
                r'.*- สหกรณ์อิสลามบารอกะฮฺ จำกัด$': 'สหกรณ์อิสลามบารอกะฮฺ จำกัด',
                r'.*- สหกรณ์อัล-อามีนสตูล จำกัด$': 'สหกรณ์อัล-อามีน สตูล จำกัด',
                r'.*- สหกรณ์เครดิตยูเนี่ยนต้นรุงก้าวไกล$': 'สหกรณ์เครดิตยูเนี่ยนต้นรุงก้าวไกล',
            },
            regex=True
        )

        df_merged['clickbank'] = df_merged['clickbank'].replace(to_replace=r'.*undefined', value='', regex=True)

        # ===== goverment_type_text normalization (ย่อ) =====
        def to_str(s): return s.astype(str).str.strip()
        def norm_lower(s): return to_str(s).str.lower()
        def to_bool(s: pd.Series) -> pd.Series:
            s_str = norm_lower(s)
            truthy = {'1','y','yes','true','t','ใช่','true-ish','ok'}
            falsy  = {'0','n','no','false','f','ไม่','none','null','na',''}
            as_num = pd.to_numeric(s_str, errors='coerce')
            num_truth = as_num.fillna(0) > 0
            cat_truth = s_str.isin(truthy)
            cat_false = s_str.isin(falsy)
            out = num_truth | cat_truth
            out = out & (~cat_false)
            return out.fillna(False)

        is_go_f = to_bool(df_merged.get('isGovernmentOfficer', ''))
        is_sp_f = to_bool(df_merged.get('is_special_campaign', ''))
        gpf_l   = norm_lower(df_merged.get('status_gpf', ''))
        gpf_yes = gpf_l.isin({'yes','y','true','t','1','ใช่','gpf','กบข'}) | (pd.to_numeric(gpf_l, errors='coerce').fillna(0) > 0)
        camp_l  = norm_lower(df_merged.get('current_campaign', ''))
        newin_s = to_str(df_merged.get('newinsurance', ''))
        idcus_s = to_str(df_merged.get('id_cus', pd.Series('', index=df_merged.index)))
        is_tnc  = newin_s.str.contains('ธนชาต', na=False)
        is_lady = camp_l.eq('lady')
        is_fng  = idcus_s.str.startswith('FNG', na=False)

        m1 = is_go_f & gpf_yes & (~is_sp_f)
        m2 = is_go_f & (~gpf_yes) & (~is_sp_f)
        m3 = is_sp_f & is_tnc & is_fng
        m4 = is_tnc & is_lady
        m5 = is_sp_f

        df_merged['goverment_type_text'] = ''
        df_merged.loc[m1, 'goverment_type_text'] = 'กบข'
        df_merged.loc[m2, 'goverment_type_text'] = 'กบข(ฟิน)'
        df_merged.loc[m3, 'goverment_type_text'] = 'customize_tnc'
        df_merged.loc[m4, 'goverment_type_text'] = 'lady'
        df_merged.loc[m5 & (df_merged['goverment_type_text'] == ''), 'goverment_type_text'] = 'สู้สุดใจ'

        # ====== ใช้ราคา: มี order_number -> ใช้ order, ถ้าไม่มี -> ใช้ pay ======
        def _normalize_digits(s: str) -> str:
            # แปลงเลขไทยเป็นอารบิก + ตัดอักขระที่ไม่ใช่ 0-9 . -
            if s is None:
                return ''
            s = str(s)
            s = s.translate(str.maketrans('๐๑๒๓๔๕๖๗๘๙', '0123456789'))
            s = s.replace(',', '').strip()
            s = re.sub(r'[^0-9.\-]', '', s)
            return s

        def _to_num(series: pd.Series) -> pd.Series:
            return pd.to_numeric(series.astype(str).map(_normalize_digits), errors='coerce')

        has_order = (
            df_merged['order_number'].astype(str).str.strip().replace({'': np.nan, '0': np.nan}).notna()
            if 'order_number' in df_merged.columns else pd.Series(False, index=df_merged.index)
        )

        # ตัวเลขจากฝั่ง order / pay
        ord_ins = _to_num(df_merged.get('show_price_ins'))
        ord_prb = _to_num(df_merged.get('show_price_prb'))
        ord_tot = _to_num(df_merged.get('show_price_total'))

        pay_ins = _to_num(df_merged.get('show_price_ins_pay'))
        pay_prb = _to_num(df_merged.get('show_price_prb_pay'))
        pay_tot = _to_num(df_merged.get('show_price_total_pay'))

        df_merged['ins_amount']   = np.where(has_order, ord_ins, pay_ins)
        df_merged['prb_amount']   = np.where(has_order, ord_prb, pay_prb)
        df_merged['total_amount'] = np.where(has_order, ord_tot, pay_tot)

        # Debug: แถวที่มี order แต่ค่าเป็น NaN (ช่วยไล่ NULL)
        try:
            anom = df_merged[has_order & (df_merged['ins_amount'].isna() | df_merged['prb_amount'].isna() | df_merged['total_amount'].isna())]
            if len(anom):
                logger.warning("⚠️ มี order_number แต่บาง amount เป็น NaN: %d แถว (ตัวอย่าง 5)", len(anom))
                logger.warning("%s", anom[['quo_num','order_number','show_price_ins','show_price_prb','show_price_total']].head(5).to_dict('records'))
        except Exception as _e:
            logger.debug("skip anom debug: %s", _e)

        # ทำความสะอาดเบอร์โทร
        if 'tel' in df_merged.columns:
            df_merged['tel'] = (
                df_merged['tel'].astype(str).str.strip().str.replace(r'\D+', '', regex=True).replace({'': None})
            )

        # ล้างค่าว่างทั่วไป
        df_merged = df_merged.replace(['nan', 'NaN', 'null', ''], np.nan)
        df_merged = df_merged.replace(r'^\s*$', np.nan, regex=True)
        df_merged = df_merged.where(pd.notnull(df_merged), None)

        # ตั้งชื่อคอลัมน์ปลายทาง (ไม่ map show_price_* → เพราะคำนวณแล้วข้างบน)
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
            'Local Broker', ''
        )

        # เตรียมคอลัมน์จำนวนเงินอื่น ๆ ให้ครบ
        amt_cols_after = ['ins_amount','prb_amount','total_amount','service_price','tax_car_price',
                          'overdue_fine_price','price_addon','payment_amount','tax_amount','ems_amount']
        for c in amt_cols_after:
            if c not in df_merged.columns:
                df_merged[c] = np.nan

        # จัดลำดับและ dedup PK
        df_merged['_nonnull_amt'] = pd.DataFrame({c: pd.to_numeric(df_merged[c], errors='coerce')
                                                  for c in amt_cols_after}).notna().sum(axis=1)
        df_merged['_sort_plan'] = pd.to_datetime(df_merged.get('quotation_date') if 'quotation_date' in df_merged.columns else df_merged.get('datestart'), errors='coerce')
        df_merged['_sort_pay']  = pd.to_datetime(df_merged.get('transaction_date') if 'transaction_date' in df_merged.columns else df_merged.get('datestart_pay'), errors='coerce')
        df_merged['_sort_ord']  = pd.to_datetime(df_merged.get('order_time') if 'order_time' in df_merged.columns else df_merged.get('datekey'), errors='coerce')

        df_merged.sort_values(by=['quotation_num','_nonnull_amt','_sort_plan','_sort_pay','_sort_ord'],
                              ascending=[True, False, False, False, False], inplace=True)
        df_merged = df_merged.drop_duplicates(subset=['quotation_num'], keep='first') \
                             .drop(columns=['_nonnull_amt','_sort_plan','_sort_pay','_sort_ord'])

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
            'display_permission','name','lastname','company','type_insurance','fin_new_group',
            'current_campaign','newinsurance','cuscode','id_cus_pay'
        ]
        df_merged.drop(columns=[c for c in cols_to_drop if c in df_merged.columns], inplace=True)

        # ✅ แปลงวันที่เก็บเป็น yyyymmdd แล้วค่อย cast Int64
        for col in ['transaction_date','order_time','quotation_date']:
            if col in df_merged.columns:
                df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce').dt.strftime('%Y%m%d')

        # ✅ installment_number map
        if 'installment_number' in df_merged.columns:
            df_merged['installment_number'] = df_merged['installment_number'].replace({'0':'1','03':'3','06':'6','08':'8'})

        # ✅ สร้าง status (เหมือนเดิม)
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
            lambda row: (str(row.get('status_fssp') or '').strip(), str(row.get('status_fsp') or '').strip()), axis=1
        )
        df_merged['status'] = df_merged['status_key'].map(status_mapping)
        df_merged.loc[df_merged['status_key'].apply(lambda x: 'cancel' in x), 'status'] = 'cancel'
        df_merged.loc[df_merged['status_key'].apply(lambda x: 'delete' in x), 'status'] = 'delete'

        if 'status_fo' in df_merged.columns:
            fo_mask = df_merged['status_fo'].notna()
            df_merged.loc[fo_mask, 'status'] = df_merged.loc[fo_mask, 'status_fo'].apply(lambda x: 'cancel' if x == '88' else x)
            df_merged.drop(columns=['status_fo'], inplace=True)

        for c in ['status_fssp', 'status_fsp', 'status_key']:
            if c in df_merged.columns:
                df_merged.drop(columns=[c], inplace=True)

        # ✅ ลบข้อมูลซ้ำ
        df_merged.drop_duplicates(subset=['quotation_num'], keep='first', inplace=True)

        # ✅ แปลง numeric
        numeric_columns = [
            'installment_number', 'show_price_check', 'ems_amount', 'service_price',
            'ins_amount', 'prb_amount', 'total_amount', 'tax_car_price', 'overdue_fine_price',
            'ins_discount', 'mkt_discount', 'payment_amount', 'price_addon', 'discount_addon',
            'goverment_discount', 'tax_amount', 'fin_goverment_discount', 'ins_goverment_discount'
        ]
        for col in numeric_columns:
            if col in df_merged.columns:
                df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce').replace([np.inf, -np.inf], None)

        # ✅ cast Int64 เฉพาะคอลัมน์วันที่/ตัวนับ
        for col in ['transaction_date','order_time','installment_number','show_price_check','quotation_date']:
            if col in df_merged.columns:
                df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce').astype('Int64')

        if 'status_detail' in df_merged.columns:
            before = len(df_merged)
            mask_test = df_merged['status_detail'].astype(str).str.contains('ทดสอบระบบ|เทสระบบ', regex=True, na=False)
            df_merged = df_merged.loc[~mask_test].copy()
            removed = before - len(df_merged)
            logger.info(f"🧪 ตัดแถว test (ทดสอบระบบ/เทสระบบ) ออก {removed:,} แถว")

        # 🧼 เก็บบ้าน: ไม่ใช้คอลัมน์ต้นทางราคาแล้ว ลบทิ้ง (ถ้ายังเหลือ)
        for c in ['show_price_ins','show_price_prb','show_price_total',
                  'show_price_ins_pay','show_price_prb_pay','show_price_total_pay']:
            if c in df_merged.columns:
                df_merged.drop(columns=c, inplace=True)

        logger.info("✅ การทำความสะอาดข้อมูลเสร็จสิ้น")
        return df_merged

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการทำความสะอาดข้อมูล: {e}")
        raise

@op
def load_sales_quotation_data(df: pd.DataFrame, batch_size: int = 10_000):
    table_name = 'fact_sales_quotation'
    pk_column = 'quotation_num'

    # --- prepare dataframe ---
    df = df[df[pk_column].notna()].copy()
    df = df[~df[pk_column].duplicated(keep='last')].copy()
    if df.empty:
        print("⚠️ No valid data to process")
        return

    table = Table(table_name, MetaData(), autoload_with=target_engine)
    table_cols = [c.name for c in table.columns]

    # ✅ ตรวจ schema ว่ามีคอลัมน์จำนวนเงินปลายทางครบ
    must_have = {'ins_amount','prb_amount','total_amount'}
    missing = must_have - set(table_cols)
    if missing:
        raise RuntimeError(f"Target table missing columns: {missing} (ตรวจ schema ของ {table_name})")

    if pk_column not in df.columns:
        raise ValueError(f"Primary key '{pk_column}' missing in dataframe")

    valid_cols = [c for c in df.columns if c in table_cols]
    df = df[valid_cols].copy()

    def to_db_value(v):
        if pd.isna(v) or v is pd.NaT or v == '':
            return None
        return v

    # plan vs coalesce cols
    plan_columns = {
        'quotation_num','quotation_date','rights_government','goverment_type',
        'quotation_num_old','type_insurance','plan_type','is_government_officer',
        'sale_team','goverment_type_text','local_broker','status','customer_name',
        'tel','big_agent','dna_fin'
    }

    insert_stmt = pg_insert(table)

    update_columns = {}
    for col in table_cols:
        if col in [pk_column, 'create_at', 'update_at']:
            continue
        if col in plan_columns:
            update_columns[col] = insert_stmt.excluded[col]  # overwrite (แม้จะ NULL)
        else:
            update_columns[col] = func.coalesce(insert_stmt.excluded[col], getattr(table.c, col))

    if 'update_at' in table_cols:
        update_columns['update_at'] = func.now()

    # อัปเดตเฉพาะเมื่อค่าเปลี่ยนจริง ๆ เพื่อลด write load
    comparable_cols = [c for c in update_columns.keys() if c in table_cols]
    distinct_checks = [getattr(table.c, c).is_distinct_from(insert_stmt.excluded[c]) for c in comparable_cols]
    where_changed = or_(*distinct_checks) if distinct_checks else None

    # ใช้ RETURNING (xmax = 0) AS inserted เพื่อนับ insert vs update
    upsert_stmt = (
        insert_stmt.on_conflict_do_update(
            index_elements=[pk_column],
            set_=update_columns,
            where=where_changed
        )
        .returning(table.c[pk_column], text("(xmax = 0) AS inserted"))
    )

    total = len(df)
    print(f"📦 Preparing to upsert {total:,} rows (batch_size={batch_size})")

    insert_count = 0
    update_count = 0

    with target_engine.begin() as conn:
        rows_iter = (df.iloc[i:i+batch_size] for i in range(0, total, batch_size))
        for i, chunk in enumerate(rows_iter, start=1):
            now_ts = pd.Timestamp.now()
            records = []
            for _, row in chunk.iterrows():
                rec = {col: to_db_value(row.get(col)) for col in chunk.columns}
                if 'create_at' in table_cols and 'create_at' not in rec:
                    rec['create_at'] = now_ts
                if 'update_at' in table_cols and 'update_at' not in rec:
                    rec['update_at'] = now_ts
                records.append(rec)

            res = conn.execute(upsert_stmt, records)
            returned = res.fetchall()
            ins = sum(1 for _, inserted in returned if inserted)
            upd = len(returned) - ins
            insert_count += ins
            update_count += upd

            print(f"✅ Batch {i}: upserted={len(returned):,} (insert={ins:,}, update={upd:,})")

    print(f"🎯 Done. Total = {insert_count+update_count:,} | insert={insert_count:,} | update={update_count:,}")

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

        # output_path = "fact_sales_quotation.xlsx"
        # df_clean.to_excel(output_path, index=False, engine='openpyxl')
        # print(f"💾 Saved to {output_path}")

        load_sales_quotation_data(df_clean)
        logger.info("🎉 completed! Data upserted to fact_sales_quotation.")
    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการประมวลผล: {e}")
        import traceback
        traceback.print_exc()
        raise