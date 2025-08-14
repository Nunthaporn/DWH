from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import create_engine, MetaData, Table, inspect, text
import logging
from datetime import datetime

# =========================
# ✅ ตั้งค่าเบื้องต้น
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# ✅ DB source (MariaDB) - เพิ่ม timeout และ connection pool
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300
    }
)
source_engine_task = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300
    }
)

# ✅ DB target (PostgreSQL) - เพิ่ม timeout และ connection pool
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={
        'connect_timeout': 60,
        'options': '-c statement_timeout=300000'  # 5 นาที
    }
)

TABLE_NAME = 'fact_sales_quotation_temp'
PK_COLUMN = 'quotation_num'
BATCH_SIZE = 5000  # ปรับได้ตามทรัพยากร

# =========================
# ✅ Extract
# =========================
@op
def extract_sales_quotation_data():
    """ดึงข้อมูลจากแหล่งต้นทาง โดย alias คอลัมน์ให้ตรงตามที่ใช้จริง"""
    try:
        logger.info("📦 เริ่มดึงข้อมูลจาก source databases...")

        # plan: alias datestart -> quotation_date
        df_plan = pd.read_sql("""
            SELECT quo_num,
                   type_insure,
                   datestart AS quotation_date,
                   id_government_officer,
                   status_gpf,
                   quo_num_old,
                   status AS status_fssp,
                   type_car,
                   chanel_key,
                   id_cus
            FROM fin_system_select_plan
            WHERE datestart BETWEEN '2025-01-01' AND '2025-08-31'
              AND id_cus NOT LIKE '%%FIN-TestApp%%'
              AND id_cus NOT LIKE '%%FIN-TestApp3%%'
              AND id_cus NOT LIKE '%%FIN-TestApp2%%'
              AND id_cus NOT LIKE '%%FIN-TestApp-2025%%'
              AND id_cus NOT LIKE '%%FIN-Tester1%%'
              AND id_cus NOT LIKE '%%FIN-Tester2%%'
        """, source_engine)

        # order: alias datekey -> order_time
        df_order = pd.read_sql("""
            SELECT quo_num,
                   order_number,
                   chanel,
                   datekey AS order_time,
                   status AS status_fo
            FROM fin_order
            WHERE quo_num IS NOT NULL
        """, source_engine_task)

        # pay: alias datestart -> transaction_date
        df_pay = pd.read_sql("""
            SELECT quo_num,
                   datestart AS transaction_date,
                   numpay,
                   show_price_ins,
                   show_price_prb,
                   show_price_total,
                   show_price_check,
                   show_price_service,
                   show_price_taxcar,
                   show_price_fine,
                   show_price_addon,
                   show_price_payment,
                   distax,
                   show_ems_price,
                   show_discount_ins,
                   discount_mkt,
                   discount_government,
                   discount_government_fin,
                   discount_government_ins,
                   coupon_addon,
                   status AS status_fsp
            FROM fin_system_pay
        """, source_engine)

        # risk / pa / health: ตั้งชื่อให้ไม่ชนกัน
        df_risk = pd.read_sql("""
            SELECT quo_num, type AS risk_type
            FROM fin_detail_plan_risk
            WHERE type = 'คอนโด'
        """, source_engine)

        df_pa = pd.read_sql("""
            SELECT quo_num, special_package AS special_package_pa
            FROM fin_detail_plan_pa
            WHERE special_package = 'CHILD'
        """, source_engine)

        df_health = pd.read_sql("""
            SELECT quo_num, special_package AS special_package_health
            FROM fin_detail_plan_health
            WHERE special_package = 'CHILD'
        """, source_engine)

        df_wp = pd.read_sql("""
            SELECT cuscode AS id_cus, display_permission
            FROM wp_users
            WHERE display_permission IN ('สำนักงานฟิน', 'หน้าร้านฟิน')
              AND cuscode NOT LIKE '%%FIN-TestApp%%'
              AND cuscode NOT LIKE '%%FIN-TestApp3%%'
              AND cuscode NOT LIKE '%%FIN-TestApp2%%'
              AND cuscode NOT LIKE '%%FIN-TestApp-2025%%'
              AND cuscode NOT LIKE '%%FIN-Tester1%%'
              AND cuscode NOT LIKE '%%FIN-Tester2%%'
        """, source_engine)

        logger.info(f"📦 Shapes: plan={df_plan.shape}, order={df_order.shape}, pay={df_pay.shape}, risk={df_risk.shape}, pa={df_pa.shape}, health={df_health.shape}, wp={df_wp.shape}")
        return df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการดึงข้อมูล: {e}")
        raise

# =========================
# ✅ Clean / Transform
# =========================
@op
def clean_sales_quotation_data(inputs):
    try:
        df_plan, df_order, df_pay, df_risk, df_pa, df_health, df_wp = inputs
        logger.info("🧹 เริ่มทำความสะอาดข้อมูล...")

        # กรอง String ว่าง/สกปรก -> NaN
        def normalize_nulls(df):
            return (df.replace(['nan', 'NaN', 'null', 'NULL', ''], np.nan)
                      .replace(r'^\s*$', np.nan, regex=True))

        df_plan  = normalize_nulls(df_plan)
        df_order = normalize_nulls(df_order)
        df_pay   = normalize_nulls(df_pay)
        df_risk  = normalize_nulls(df_risk)
        df_pa    = normalize_nulls(df_pa)
        df_health= normalize_nulls(df_health)
        df_wp    = normalize_nulls(df_wp)

        # ✅ Dedupe df_pay โดยใช้ transaction_date แล้วเก็บตัวล่าสุดต่อ quo_num
        if 'transaction_date' in df_pay.columns:
            df_pay['transaction_date'] = pd.to_datetime(df_pay['transaction_date'], errors='coerce')
            df_pay = (df_pay.sort_values('transaction_date')
                            .drop_duplicates(subset='quo_num', keep='last'))

        # ✅ Merge
        df_merged = df_plan.merge(df_order, on='quo_num', how='left')
        df_merged = df_merged.merge(df_pay, on='quo_num', how='left', suffixes=('', '_pay'))
        df_merged = df_merged.merge(df_risk, on='quo_num', how='left')
        df_merged = df_merged.merge(df_pa, on='quo_num', how='left')
        df_merged = df_merged.merge(df_health, on='quo_num', how='left')
        df_merged = df_merged.merge(df_wp, on='id_cus', how='left')

        logger.info(f"📊 Shape after merge (before date handling): {df_merged.shape}")

        # ✅ Rename columns ที่จำเป็น (ส่วน alias จาก SQL แล้ว ไม่ต้อง map ซ้ำ)
        col_map = {
            'quo_num': 'quotation_num',
            'type_insure': 'type_insurance',
            'id_government_officer': 'rights_government',
            'status_gpf': 'goverment_type',
            'quo_num_old': 'quotation_num_old',
            'numpay': 'installment_number',
            'show_price_ins': 'ins_amount',
            'show_price_prb': 'prb_amount',
            'show_price_total': 'total_amount',
            'show_price_check': 'show_price_check',
            'show_price_service': 'service_price',
            'show_price_taxcar': 'tax_car_price',
            'show_price_fine': 'overdue_fine_price',
            'show_price_addon': 'price_addon',
            'show_price_payment': 'payment_amount',
            'distax': 'tax_amount',
            'show_ems_price': 'ems_amount',
            'show_discount_ins': 'ins_discount',
            'discount_mkt': 'mkt_discount',
            'discount_government': 'goverment_discount',
            'discount_government_fin': 'fin_goverment_discount',
            'discount_government_ins': 'ins_goverment_discount',
            'coupon_addon': 'discount_addon',
            'chanel': 'contact_channel'
        }
        df_merged.rename(columns=col_map, inplace=True)

        # ✅ Parse วันที่เป็น datetime
        for col in ['quotation_date', 'transaction_date', 'order_time']:
            if col in df_merged.columns:
                df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce')

        # ✅ Fallback วันที่: transaction_date <- order_time <- quotation_date
        before_fallback_na = df_merged['transaction_date'].isna().sum() if 'transaction_date' in df_merged.columns else None
        df_merged['transaction_date'] = (
            df_merged.get('transaction_date')
            .fillna(df_merged.get('order_time'))
            .fillna(df_merged.get('quotation_date'))
        )
        after_fallback_na = df_merged['transaction_date'].isna().sum()
        logger.info(f"🕒 transaction_date NaN ก่อน fallback = {before_fallback_na}, หลัง fallback = {after_fallback_na}")

        # # ✅ ถ้าต้องการเก็บเฉพาะที่มีวันที่จริง ค่อยกรองหลัง fallback (จะไม่หายเยอะแล้ว)
        # before_drop = len(df_merged)
        # df_merged = df_merged[df_merged['transaction_date'].notna()].copy()
        # after_drop = len(df_merged)
        # logger.info(f"🧹 ลบแถวที่ไม่มีวันที่หลัง fallback: {before_drop - after_drop}")

        # ✅ ฟอร์แมตวันที่เป็น yyyymmdd (string)
        for col in ['quotation_date', 'transaction_date', 'order_time']:
            if col in df_merged.columns:
                df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce').dt.strftime('%Y%m%d')
                df_merged[col] = df_merged[col].where(pd.notnull(df_merged[col]), None)

        # ✅ sale_team
        def assign_sale_team(row):
            id_cus = str(row.get('id_cus') or '')
            type_insurance = str(row.get('type_insurance') or '').strip().lower()
            type_car = str(row.get('type_car') or '').strip().lower()
            chanel_key = str(row.get('chanel_key') or '').strip()
            sp_pa = str(row.get('special_package_pa') or '').strip().upper()
            sp_health = str(row.get('special_package_health') or '').strip().upper()
            risk_type = str(row.get('risk_type') or '').strip()

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
            if chanel_key == 'WEB-SUBBROKER':
                return 'Subbroker'
            if risk_type == 'คอนโด':
                return 'ประกันคอนโด'
            if sp_pa == 'CHILD' or sp_health == 'CHILD':
                return 'ประกันเด็ก'
            if row.get('display_permission') in ['หน้าร้านฟิน', 'สำนักงานฟิน']:
                return 'หน้าร้าน'
            if not type_insurance and not type_car:
                return 'N/A'
            return 'Non Motor'

        df_merged['sale_team'] = df_merged.apply(assign_sale_team, axis=1)

        # ✅ คอลัมน์ที่ไม่ใช้ทิ้ง
        df_merged.drop(columns=[
            'id_cus', 'type_car', 'chanel_key',
            'special_package_pa', 'special_package_health',
            'risk_type', 'display_permission'
        ], errors='ignore', inplace=True)

        # ✅ installment_number map
        if 'installment_number' in df_merged.columns:
            installment_mapping = {'0': '1', '03': '3', '06': '6', '08': '8'}
            df_merged['installment_number'] = df_merged['installment_number'].astype(str).replace(installment_mapping)

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
                ('active', 'verify'): '3',
                ('active', ''): '8',
                ('active', 'success'): '8',
                ('active', 'success-waitinstall'): '8',
                ('active', 'sendpay'): '2',
                ('delete', 'verify'): 'delete',
                ('wait', 'verify'): '2',
                ('active', 'cancel'): 'cancel',
                ('wait-pay', ''): '1',
                ('', 'verify'): '2',
                ('tran-succ', 'wait-key'): '2',
                ('', 'cancel'): 'cancel',
                ('delelte', 'sendpay'): 'delete',
                ('cancel', 'success'): 'cancel',
                ('sendpay', ''): '2',
                ('wait-cancel', 'wait'): 'cancel',
                ('cancel', 'sendpay'): 'cancel',
                ('cancel', 'wait'): 'cancel',
                ('active', 'wait'): '1',
                ('tran-succ', 'verify'): '2',
                ('active', 'verify-wait'): '1',
                ('cancel', 'verify'): 'cancel',
                ('wait', 'cancel'): 'cancel',
                ('tran-succ', 'cancel'): 'cancel',
                ('', 'success'): '8',
                ('tran-succ', 'wait-confirm'): '2',
                ('wait-key', 'sendpay'): '2',
                ('wait-key', 'wait-key'): '1',
                ('wait-pay', 'sendpay'): '2',
            }

        status_mapping = create_status_mapping()
        df_merged['status_key'] = df_merged.apply(
            lambda row: (
                str(row.get('status_fssp') or '').strip(),
                str(row.get('status_fsp') or '').strip()
            ), axis=1
        )
        df_merged['status'] = df_merged['status_key'].map(status_mapping)

        # override ด้วย status_fo (เดิม)
        if 'status_fo' in df_merged.columns:
            fo_mask = df_merged['status_fo'].notna()
            df_merged.loc[fo_mask, 'status'] = df_merged.loc[fo_mask, 'status_fo'].apply(
                lambda x: 'cancel' if x == '88' else x
            )

        # delete/cancel priority
        df_merged['has_delete'] = df_merged['status_key'].apply(lambda t: isinstance(t, tuple) and ('delete' in t))
        df_merged['has_cancel'] = df_merged['status_key'].apply(lambda t: isinstance(t, tuple) and ('cancel' in t))
        df_merged.loc[df_merged['has_delete'] == True, 'status'] = 'delete'
        df_merged.loc[(df_merged['has_delete'] != True) & (df_merged['has_cancel'] == True), 'status'] = 'cancel'
        df_merged.drop(columns=['has_delete', 'has_cancel'], inplace=True, errors='ignore')

        # ✅ numeric cast
        numeric_cols = [
            'installment_number', 'show_price_check', 'price_product', 'ems_amount', 'service_price',
            'ins_amount', 'prb_amount', 'total_amount', 'tax_car_price', 'overdue_fine_price',
            'ins_discount', 'mkt_discount', 'payment_amount', 'price_addon', 'discount_addon',
            'goverment_discount', 'tax_amount', 'fin_goverment_discount', 'ins_goverment_discount'
        ]
        for col in numeric_cols:
            if col in df_merged.columns:
                df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce').replace([np.inf, -np.inf], np.nan)

        # ✅ สุดท้ายแปลง NaN/NaT -> None สำหรับโหลดเข้า DB
        df_merged = df_merged.where(pd.notnull(df_merged), None)

        logger.info("✅ การทำความสะอาดข้อมูลเสร็จสิ้น")
        logger.info(f"📏 Final rows after clean: {len(df_merged)}")
        return df_merged

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการทำความสะอาดข้อมูล: {e}")
        raise

# =========================
# ✅ Load (Batch Upsert เร็วขึ้น)
# =========================
def chunker(df: pd.DataFrame, size: int):
    for start in range(0, len(df), size):
        yield df.iloc[start:start+size].copy()

@op
def load_sales_quotation_data(df: pd.DataFrame):
    table_name = 'fact_sales_quotation_temp'
    pk_column = 'quotation_num'
    BATCH_SIZE = 5000

    if pk_column not in df.columns:
        raise ValueError(f"ไม่พบคอลัมน์ key '{pk_column}' ใน DataFrame")

    # เอาซ้ำออก + เอา key ว่างออก
    df = df[~df[pk_column].duplicated(keep='first')].copy()
    df = df[df[pk_column].notna()].copy()
    if df.empty:
        logging.warning("⚠️ ไม่มีข้อมูลที่พร้อมจะโหลด")
        return

    logging.info(f"📊 เตรียม upsert {len(df)} แถว (batch={BATCH_SIZE})")

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=target_engine)

    now_ts = pd.Timestamp.now()

    def chunker(_df, size):
        for start in range(0, len(_df), size):
            yield _df.iloc[start:start+size].copy()

    with target_engine.begin() as conn:
        for part in chunker(df, BATCH_SIZE):
            # แปลง NaN/NaT -> None และ set เฉพาะ create_at ตอน INSERT
            records = []
            for _, row in part.iterrows():
                rec = {}
                for col, val in row.items():
                    if pd.isna(val) or val is pd.NaT or val == '':
                        rec[col] = None
                    else:
                        rec[col] = val
                # ✅ ไม่แตะต้อง datestart
                # ใส่ create_at เฉพาะตอน insert เท่านั้น
                rec.setdefault('create_at', now_ts)
                records.append(rec)

            stmt = pg_insert(table)
            # ✅ ไม่แตะต้อง datestart ตอน UPDATE และไม่ส่งค่ามันทิ้งเข้าไป
            update_columns = {
                c.name: stmt.excluded[c.name]
                for c in table.columns
                if c.name not in [pk_column, 'create_at', 'datestart']
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=[pk_column],
                set_=update_columns
            )
            conn.execute(stmt, records)

    logging.info("✅ Upsert เสร็จสมบูรณ์ (ไม่ยุ่งกับ datestart)")


# =========================
# ✅ Dagster Job
# =========================
@job
def fact_sales_quotation_etl():
    data = extract_sales_quotation_data()
    df_clean = clean_sales_quotation_data(data)
    load_sales_quotation_data(df_clean)

# =========================
# ✅ Script run
# =========================
if __name__ == "__main__":
    try:
        logger.info("🚀 เริ่มการประมวลผล fact_sales_quotation...")

        extracted = extract_sales_quotation_data()
        df_clean = clean_sales_quotation_data(extracted)

        # Export เพื่อ debug (ถ้าต้องการ)
        out_path = "fact_sales_quotation.xlsx"
        df_clean.to_excel(out_path, index=False, engine='openpyxl')
        logger.info(f"💾 บันทึกไฟล์ {out_path}")

        # load_sales_quotation_data(df_clean)

        logger.info("🎉 completed! Data upserted to fact_sales_quotation_temp.")

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการประมวลผล: {e}")
        import traceback
        traceback.print_exc()
        raise
