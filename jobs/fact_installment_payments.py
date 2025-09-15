# -*- coding: utf-8 -*-
from dagster import op, job
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, text, func, or_, bindparam
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import List, Dict

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    ZoneInfo = None

# =======================================
#           ENV & ENGINES (GLOBAL)
# =======================================
load_dotenv()

MYSQL_SOURCE_URL = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)
MYSQL_TASK_URL = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task"
)
PG_TARGET_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}"
    f"@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

# หมายเหตุ: **อย่า dispose()** engines เหล่านี้ระหว่างการทำงาน
source_engine = create_engine(
    MYSQL_SOURCE_URL,
    pool_size=10, max_overflow=20, pool_timeout=30, pool_recycle=1800, pool_pre_ping=True, echo=False,
    connect_args={'connect_timeout': 60, 'autocommit': False, 'charset': 'utf8mb4'}
)
task_engine = create_engine(
    MYSQL_TASK_URL,
    pool_size=10, max_overflow=20, pool_timeout=30, pool_recycle=1800, pool_pre_ping=True, echo=False,
    connect_args={'connect_timeout': 60, 'autocommit': False, 'charset': 'utf8mb4'}
)
target_engine = create_engine(
    PG_TARGET_URL,
    pool_size=10, max_overflow=20, pool_timeout=30, pool_recycle=1800, pool_pre_ping=True, echo=False,
    connect_args={
        "connect_timeout": 30,
        "application_name": "fact_installment_payments_etl_fast",
        "options": "-c statement_timeout=300000 -c idle_in_transaction_session_timeout=300000",
    },
)

# =======================================
#               HELPERS (DATE)
# =======================================
def today_range_th():
    """คืนค่า (start_dt, end_dt) เป็น naive datetime ของช่วง 'วันนี้' ตาม Asia/Bangkok"""
    if ZoneInfo is not None:
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

# =======================================
#               HELPERS (DB)
# =======================================
def read_df(sql: str, eng, params: dict | None = None) -> pd.DataFrame:
    """อ่านด้วย single connection (ไม่ dispose pool)"""
    with eng.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def read_df_in(sql: str, eng, param_name: str, values: List, extra_params: dict | None = None) -> pd.DataFrame:
    """อ่าน query ที่มี IN (:param) แบบ expanding (SQLAlchemy)"""
    if not values:
        return pd.DataFrame()
    stmt = text(sql).bindparams(bindparam(param_name, expanding=True))
    with eng.connect() as conn:
        return pd.read_sql(stmt, conn, params={param_name: list(values), **(extra_params or {})})

def chunker(df: pd.DataFrame, size: int = 20000):
    for i in range(0, len(df), size):
        yield df.iloc[i:i+size]

# =======================================
#        HELPERS (PIVOT/TRANSFORM)
# =======================================
MAP12 = {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6,
         'seven':7,'eight':8,'nine':9,'ten':10,'eleven':11,'twelve':12}

def unpivot_installment(df_inst: pd.DataFrame) -> pd.DataFrame:
    if df_inst.empty:
        return pd.DataFrame(columns=['quo_num','numpay','installment_number','installment_amount','due_date'])

    money = df_inst.filter(regex=r'^money_(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)$')
    dates = df_inst.filter(regex=r'^date_(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)$')

    m = money.melt(ignore_index=False, var_name='mcol', value_name='installment_amount')
    d = dates.melt(ignore_index=False, var_name='dcol', value_name='due_date')

    base = df_inst[['quo_num','numpay']]
    out = base.join(m, how='right').join(d['due_date']).reset_index(drop=True)

    out['installment_number'] = out['mcol'].str.extract(r'money_(\w+)')[0].map(MAP12).astype('Int64')
    out['numpay'] = pd.to_numeric(out['numpay'], errors='coerce')
    out = out[out['installment_number'] <= out['numpay']]

    out['installment_amount'] = pd.to_numeric(out['installment_amount'].astype(str).str.replace(',', ''), errors='coerce')
    out['due_date'] = pd.to_datetime(out['due_date'], errors='coerce')

    return out[['quo_num','installment_number','installment_amount','due_date']]

def unpivot_finance(df_fin: pd.DataFrame) -> pd.DataFrame:
    """
    แปลงคอลัมน์ moneypay_* / datepay_* ให้เป็นแนวยาว และ map เป็น installment_number 1..12
    - รองรับกรณีบางคอลัมน์ไม่มีในตาราง (จะข้ามไป)
    - แปลงตัวเลข/วันที่แบบเวคเตอร์
    """
    if df_fin.empty or 'order_number' not in df_fin.columns:
        return pd.DataFrame(columns=['order_number','installment_number','payment_amount','payment_date'])

    money_cols_all = [f'moneypay_{k}' for k in MAP12.keys()]
    date_cols_all  = [f'datepay_{k}'  for k in MAP12.keys()]
    money = df_fin[[c for c in money_cols_all if c in df_fin.columns]].copy()
    dates = df_fin[[c for c in date_cols_all  if c in df_fin.columns]].copy()

    # ถ้าไม่มีทั้ง money/date ก็คืน empty
    if money.empty and dates.empty:
        return pd.DataFrame(columns=['order_number','installment_number','payment_amount','payment_date'])

    m = money.melt(ignore_index=False, var_name='mcol', value_name='payment_amount')
    d = dates.melt(ignore_index=False, var_name='dcol', value_name='payment_date')

    base = df_fin[['order_number']]
    out = base.join(m, how='right').join(d['payment_date'], how='left').reset_index(drop=True)

    out['installment_number'] = out['mcol'].str.extract(r'moneypay_(\w+)')[0].map(MAP12).astype('Int64')
    out['payment_amount'] = pd.to_numeric(out['payment_amount'].astype(str).str.replace(',', ''), errors='coerce')

    out['payment_date'] = pd.to_datetime(out['payment_date'], errors='coerce')
    mask_2026 = out['payment_date'].dt.year.eq(2026)
    if mask_2026.any():
        out.loc[mask_2026, 'payment_date'] = out.loc[mask_2026, 'payment_date'] - pd.offsets.DateOffset(years=1)

    return out[['order_number','installment_number','payment_amount','payment_date']]

def unpivot_bill(df_bill: pd.DataFrame) -> pd.DataFrame:
    if df_bill.empty or 'order_number' not in df_bill.columns:
        return pd.DataFrame(columns=['order_number','installment_number','payment_proof'])

    cols = [c for c in df_bill.columns if c.startswith('bill_receipt')]
    if not cols:
        return pd.DataFrame(columns=['order_number','installment_number','payment_proof'])

    b = df_bill.melt(id_vars=['order_number'], value_vars=cols, var_name='col', value_name='payment_proof')
    b = b[b['payment_proof'].notna()]
    b['installment_number'] = b['col'].str.extract(r'bill_receipt(\d*)')[0].replace('', '1').astype(int)
    return b[['order_number','installment_number','payment_proof']]

def finalize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """จัดรูปแบบคอลัมน์สำหรับโหลดเข้า Postgres"""
    if df.empty:
        return df

    for col in ['due_date', 'payment_date']:
        if col in df.columns:
            s = pd.to_datetime(df[col], errors='coerce')
            df[col] = s.dt.strftime('%Y%m%d')
            df[col] = df[col].where(s.notna(), None)

    df = df.where(pd.notna(df), None)

    if 'installment_number' in df.columns:
        df['installment_number'] = pd.to_numeric(df['installment_number'], errors='coerce').astype('Int64')
        df['installment_number'] = df['installment_number'].where(pd.notna(df['installment_number']), None)

    return df

# =======================================
#     UPSERT HELPERS (PostgreSQL)
# =======================================
def clean_records_for_db(records: List[Dict]) -> List[Dict]:
    """แปลง NaN/NaT และ numpy scalar ให้เป็นชนิด Python + None ที่ DB รับได้"""
    def to_native(v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        if isinstance(v, (np.bool_,)):
            return bool(v)
        return v
    return [{k: to_native(v) for k, v in r.items()} for r in records]

def build_conditional_upsert(table, pk_cols: List[str], update_allow_cols: List[str], records: List[Dict]):
    """
    UPSERT อัปเดตเมื่อค่าต่างจริง (NULL-safe) และไม่ทับด้วย NULL
    update_at จะเปลี่ยนจริงเฉพาะตอนที่ค่ามีการอัปเดต
    """
    stmt = pg_insert(table).values(records)
    excluded = stmt.excluded

    set_map = {col: func.coalesce(getattr(excluded, col), getattr(table.c, col))
               for col in update_allow_cols}
    if 'update_at' in table.c:
        set_map['update_at'] = func.now()

    change_conds = [
        func.coalesce(getattr(excluded, col), getattr(table.c, col)).is_distinct_from(getattr(table.c, col))
        for col in update_allow_cols
    ]
    where_clause = or_(*change_conds) if change_conds else text("FALSE")

    return stmt.on_conflict_do_update(
        index_elements=pk_cols,
        set_=set_map,
        where=where_clause
    )

# =======================================
#                 EXTRACT
# =======================================
@op
def extract_installment_data():
    start_dt, end_dt = today_range_th()

    print("📊 Load target quo_num ...")
    df_plan = read_df("""
        SELECT quo_num
        FROM fin_system_select_plan
        WHERE datestart >= :d1 AND datestart < :d2
    """, source_engine, {"d1": start_dt, "d2": end_dt})

    if df_plan.empty:
        print("⚠️ No target plan rows.")
        return tuple(pd.DataFrame() for _ in range(7))

    quo_list = df_plan['quo_num'].dropna().astype(str).unique().tolist()

    print("📊 Load orders for target quo_num ...")
    df_order = read_df_in("""
        SELECT quo_num, order_number
        FROM fin_order
        WHERE quo_num IN :quo
    """, task_engine, "quo", quo_list)

    print("📊 Load installments ...")
    df_inst = read_df_in("""
        SELECT quo_num, numpay,
               money_one, money_two, money_three, money_four, money_five, money_six,
               money_seven, money_eight, money_nine, money_ten, money_eleven, money_twelve,
               date_one,  date_two,  date_three,  date_four,  date_five,  date_six,
               date_seven, date_eight, date_nine, date_ten, date_eleven, date_twelve
        FROM fin_installment
        WHERE quo_num IN :quo
    """, source_engine, "quo", quo_list)

    orders = df_order['order_number'].dropna().astype(str).unique().tolist() if not df_order.empty else []

    if orders:
        print(f"📊 Load finance ({len(orders)} orders) ...")
        df_fin = read_df_in("SELECT * FROM fin_finance WHERE order_number IN :orders",
                            task_engine, "orders", orders)
        print("📊 Load bill ...")
        with task_engine.connect() as conn:
            df_bill_all = pd.read_sql(
                text("SELECT * FROM fin_bill WHERE order_number IN :orders").bindparams(bindparam("orders", expanding=True)),
                conn, params={"orders": orders}
            )
        bill_cols = [
            "order_number",
            "bill_receipt", "bill_receipt2","bill_receipt3","bill_receipt4","bill_receipt5","bill_receipt6",
            "bill_receipt7","bill_receipt8","bill_receipt9","bill_receipt10","bill_receipt11","bill_receipt12"
        ]
        avail_cols = [c for c in bill_cols if c in df_bill_all.columns]
        df_bill = df_bill_all[avail_cols].copy()

        print("📊 Load late fee ...")
        df_fee = read_df_in("""
            SELECT orderNumber, penaltyPay, numPay
            FROM FIN_Account_AttachSlip_PathImageSlip
            WHERE checkPay IN ('ค่าปรับ','ค่างวด/ค่าปรับ') AND orderNumber IN :orders
        """, task_engine, "orders", orders)
    else:
        df_fin = pd.DataFrame()
        df_bill = pd.DataFrame()
        df_fee = pd.DataFrame()

    print("📊 Load test quotes ...")
    df_test = read_df("""
        SELECT quo_num
        FROM fin_system_select_plan
        WHERE name IN ('ทดสอบ','test')
    """, source_engine)

    print(f"✅ Loaded: plan({len(df_plan)}), inst({len(df_inst)}), order({len(df_order)}), "
          f"finance({len(df_fin)}), bill({len(df_bill)}), late_fee({len(df_fee)}), test({len(df_test)})")

    return df_plan, df_inst, df_order, df_fin, df_bill, df_fee, df_test

# =======================================
#                 TRANSFORM
# =======================================
@op
def clean_installment_data(inputs):
    df_plan, df_inst, df_order, df_fin, df_bill, df_fee, df_test = inputs

    print("🔄 Unpivot & basic transforms ...")
    df_inst_long = unpivot_installment(df_inst)
    if df_inst_long.empty:
        print("⚠️ No installment rows after unpivot.")
        empty_cols = ['quotation_num','installment_number','due_date','installment_amount',
                      'payment_date','payment_amount','late_fee','total_paid','payment_proof',
                      'payment_status','order_number']
        return pd.DataFrame(columns=empty_cols)

    # join order_number
    df_join = df_inst_long.merge(df_order, on='quo_num', how='left')

    # finance (payments)
    df_pay = unpivot_finance(df_fin)

    # bill (proof)
    df_proof = unpivot_bill(df_bill)

    # merge
    df = df_join.merge(df_pay, on=['order_number','installment_number'], how='left') \
                .merge(df_proof, on=['order_number','installment_number'], how='left')

    # late fee
    if not df_fee.empty:
        fee = df_fee.rename(columns={'orderNumber':'order_number','penaltyPay':'late_fee','numPay':'installment_number'}).copy()
        fee['installment_number'] = pd.to_numeric(fee['installment_number'], errors='coerce').astype('Int64')
        fee = fee.drop_duplicates()
        df['installment_number'] = pd.to_numeric(df['installment_number'], errors='coerce').astype('Int64')
        df = df.merge(fee[['order_number','installment_number','late_fee']], on=['order_number','installment_number'], how='left')
    else:
        df['late_fee'] = 0

    # numeric clean
    df['installment_amount'] = pd.to_numeric(df['installment_amount'], errors='coerce')
    df['payment_amount'] = pd.to_numeric(df['payment_amount'], errors='coerce')
    df['late_fee'] = pd.to_numeric(df['late_fee'], errors='coerce')

    # total_paid
    lf = df['late_fee'].fillna(0)
    df['total_paid'] = np.where(
        lf.eq(0),
        df['installment_amount'],
        np.where(df['payment_amount'].notna(),
                 df['payment_amount'] + lf,
                 df['installment_amount'] + lf)
    )
    na_all = df[['installment_amount','payment_amount','late_fee']].isna().all(axis=1)
    df.loc[na_all, 'total_paid'] = None

    # payment_status
    due = pd.to_datetime(df['due_date'], errors='coerce')
    pay = pd.to_datetime(df['payment_date'], errors='coerce')
    today = pd.Timestamp(datetime.now().date())

    no_payment = df['payment_amount'].isna() & df['payment_proof'].isna()
    is_overdue = due.notna() & (today > due.dt.normalize())
    is_late = pay.notna() & due.notna() & (pay.dt.normalize() > due.dt.normalize())

    df['payment_status'] = 'ยังไม่ชำระ'
    df.loc[no_payment & is_overdue, 'payment_status'] = 'เกินกำหนดชำระ'
    df.loc[(df['payment_amount'].notna() | df['payment_proof'].notna()) & ~is_late, 'payment_status'] = 'ชำระแล้ว'
    df.loc[(df['payment_amount'].notna() | df['payment_proof'].notna()) & is_late, 'payment_status'] = 'ชำระล่าช้า'

    # filter test quotes & invalid
    if not df_test.empty and 'quo_num' in df_test.columns:
        df = df[~df['quo_num'].isin(df_test['quo_num'])]

    df = df[df['quo_num'].notna() & (df['quo_num'].astype(str) != 'undefined')]

    # rename + finalize formats
    df = df.rename(columns={'quo_num': 'quotation_num'})
    df_final = df[['quotation_num','installment_number','due_date','installment_amount',
                   'payment_date','payment_amount','late_fee','total_paid',
                   'payment_proof','payment_status','order_number']].copy()

    df_final = finalize_formats(df_final)

    print(f"✅ Final rows ready for load: {len(df_final)}")
    return df_final

# =======================================
#                 LOAD
# =======================================
@op
def load_installment_data(df: pd.DataFrame):
    table_name = 'fact_installment_payments'
    pk_column = ['quotation_num', 'installment_number']

    if df.empty:
        print("⚠️ Nothing to load.")
        return

    print(f"🔍 Before loading - DataFrame shape: {df.shape}")
    print(f"🔍 Before loading - Columns: {list(df.columns)}")

    print("\n🔍 NaN check before loading:")
    for col in df.columns:
        n = df[col].isna().sum()
        if n:
            print(f"  - {col}: {n} NaN values")

    print("\n🧹 Final data cleaning before loading...")
    df_clean = df.copy()

    # แปลงสตริง nan/null/none/undefined -> None
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            s = df_clean[col].astype(str)
            mask = s.str.lower().isin(['nan', 'null', 'none', 'undefined'])
            if mask.any():
                print(f"  🔄 Converting {mask.sum()} string NaN-like values to None in {col}")
                df_clean.loc[mask, col] = None

    # ตรวจ PK
    for col in pk_column:
        if col not in df_clean.columns:
            print(f"❌ Missing primary key column in DataFrame: {col}")
            print(f"Available columns: {list(df_clean.columns)}")
            return

    # กรองแถวที่ PK เป็น null และกันซ้ำ
    df_clean = df_clean[df_clean[pk_column].notna().all(axis=1)].copy()
    if df_clean.empty:
        print("⚠️ DataFrame is empty after filtering null primary key values. No data to process.")
        return
    before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=pk_column, keep='last').copy()
    print(f"🔍 After removing duplicate composite keys in batch: {before} -> {len(df_clean)} rows")

    # ===== ใช้ target_engine ตรง ๆ (ไม่ต้องเรียก () และไม่ dispose) =====
    meta = MetaData()
    table = Table(table_name, meta, autoload_with=target_engine)
    update_cols = [c.name for c in table.columns if c.name not in pk_column + ['create_at', 'update_at']]

    print("📤 Loading with conditional UPSERT (insert new, update only when changed)...")

    total = len(df_clean)
    done = 0
    batch_size = 20000  # ปรับได้ตามสเปค

    # ตั้งค่า session 一ครั้ง
    with target_engine.begin() as conn:
        conn.execute(text("SET statement_timeout = 300000"))
        conn.execute(text("SET idle_in_transaction_session_timeout = 300000"))

    for batch in chunker(df_clean, batch_size):
        records = clean_records_for_db(batch.to_dict(orient='records'))

        # ถ้าให้ DB ใส่ create_at เอง แนะนำมี DEFAULT ในตาราง แล้วไม่ต้องเติมจากโค้ด
        stmt = build_conditional_upsert(
            table=table,
            pk_cols=pk_column,
            update_allow_cols=update_cols,
            records=records
        )

        # ทำทีละ batch พร้อม retry เบา ๆ
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                with target_engine.begin() as conn:
                    conn.execute(stmt)
                break
            except Exception as e:
                print(f"    ❌ Error upserting batch (attempt {attempt}/{max_retries}): {e}")
                if attempt == max_retries:
                    print("    ⚠️ Max retries reached for this batch. Skipping.")
                else:
                    time.sleep(2 ** attempt)

        done += len(batch)
        print(f"   ✅ upserted {done}/{total}")

    print("✅ Insert/Update completed (conditional upsert)")

@job
def fact_installment_payments_etl():
    load_installment_data(clean_installment_data(extract_installment_data()))
        
if __name__ == "__main__":
    df_raw = extract_installment_data()

    df_clean = clean_installment_data((df_raw))

    # output_path = "fact_installment_payments.xlsx"
    # df_clean.to_excel(output_path, index=False, engine='openpyxl')
    # print(f"💾 Saved to {output_path}")

    load_installment_data(df_clean)
    print("🎉 completed! Data upserted to fact_installment_payments.")