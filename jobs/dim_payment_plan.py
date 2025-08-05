from dagster import op, job
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta

# ✅ Load .env
load_dotenv()

# ✅ DB source (MariaDB)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)

# ✅ DB target (PostgreSQL)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance?connect_timeout=60"
)

@op
def extract_payment_data():
    now = datetime.now()

    start_time = now.replace(minute=0, second=0, microsecond=0)
    end_time = now.replace(minute=59, second=59, microsecond=999999)

    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    query1 = """
        SELECT quo_num, chanel_main, clickbank, chanel, numpay, condition_install
        FROM fin_system_pay
        WHERE update_at BETWEEN '{start_str}' AND '{end_str}'
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df_pay = pd.read_sql(query1, source_engine)

    query2 = """
        SELECT quo_num, status_paybill
        FROM fininsurance_task.fin_order
        WHERE type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df_order = pd.read_sql(query2, source_engine)

    df = pd.merge(df_pay, df_order, on='quo_num', how='left')

    print("📦 df_pay:", df_pay.shape)
    print("📦 df_order:", df_order.shape)
    print("📦 df:", df.shape)

    return df

@op
def clean_payment_data(df: pd.DataFrame):
    # ✅ แปลง string "NaN", " nan ", "NaN " ให้เป็น np.nan
    df = df.map(lambda x: np.nan if isinstance(x, str) and x.strip().lower() == "nan" else x)

    # ✅ เติมช่องว่างเป็น '' และเปลี่ยน type → str
    for col in ['chanel', 'chanel_main', 'clickbank']:
        df[col] = df[col].fillna('').astype(str).str.strip()
        
    ch = df['chanel'].str.lower()
    chm = df['chanel_main'].str.lower()
    cb = df['clickbank'].str.lower()

    conditions = [
        ch == 'เข้าฟิน',
        ch == 'เข้าประกัน',
        (chm.isin(['ผ่อนบัตรเครดิต', 'ผ่อนบัตร']) & cb.isin(['creditcard', '']) & ch.eq('ผ่อนบัตร')),
        (chm.eq('ตัดบัตรเครดิต') & cb.isin(['']) & ch.eq('ผ่อนบัตร')),
        (chm.eq('ผ่อนโอน') & cb.isin(['qrcode']) & ch.eq('ผ่อนโอน')),
        (chm.eq('ผ่อนโอน') & cb.str.startswith('ธนาคาร') & ch.eq('ผ่อนบัตร')),
        (chm.eq('ตัดบัตรเครดิต') & cb.isin(['creditcard', '']) & ch.eq('ออนไลน์')),
        (chm.eq('ตัดบัตรเครดิต') & cb.str.startswith('ธนาคาร') & ch.eq('ออนไลน์')),
        (chm.eq('ผ่อนบัตรเครดิต') & cb.isin(['qrcode','creditcard','']) & ch.eq('ออนไลน์')),
        (chm.eq('ผ่อนบัตรเครดิต') & cb.str.startswith('ธนาคาร') & ch.eq('ออนไลน์')),
        (chm.eq('โอนเงิน') & cb.str.startswith('ธนาคาร') & ch.eq('ออนไลน์')),
        (chm.eq('ตัดบัตรเครดิต') & cb.eq('') & ch.eq('ตัดบัตรเครดิต')),
        (chm.eq('ผ่อนชำระ') & (cb.isin(['qrcode', '']) | cb.str.startswith('ธนาคาร')) & ch.eq('ผ่อนโอน')),
        (chm.eq('ผ่อนโอน') & cb.str.startswith('ธนาคาร') & ch.eq('ผ่อนโอน')),
    ]

    choices = [
        'เข้าฟิน',
        'เข้าประกัน',
        *(['เข้าฟิน'] * (len(conditions) - 2)),
    ]

    df['chanel'] = np.select(conditions, choices, default=df['chanel'])

    # ✅ Generate payment_channel (ยังใช้ apply เพราะ logic ซับซ้อน)
    def determine_payment_channel(row):
        ch_main = str(row['chanel_main']).strip().lower()
        cb_raw = row['clickbank']
        cb = str(cb_raw).strip().lower()
        is_cb_empty = pd.isna(cb_raw) or cb == ''

        if ch_main in ['ตัดบัตรเครดิต', 'ผ่อนบัตร', 'ผ่อนบัตรเครดิต', 'ผ่อนชำระ']:
            if 'qrcode' in cb:
                return 'QR Code'
            elif 'creditcard' in cb:
                return '2C2P'
            else:
                return 'ตัดบัตรกับฟิน'
        if ch_main in ['โอนเงิน', 'ผ่อนโอน']:
            if 'qrcode' in cb:
                return 'QR Code'
            else:
                return 'โอนเงิน'
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

    df['payment_channel'] = df.apply(determine_payment_channel, axis=1)

    # ✅ Clean + rename
    df.drop(columns=['chanel_main', 'clickbank', 'condition_install'], inplace=True)
    df = df.rename(columns={
        'quo_num': 'quotation_num',
        'numpay': 'installment_number',
        'chanel': 'payment_reciever',
        'status_paybill': 'payment_type'
    })

    # ✅ Clean values
    df = df[~df['quotation_num'].str.endswith('-r', na=False)]
    df['installment_number'] = pd.to_numeric(df['installment_number'], errors='coerce').fillna(0).astype(int)
    df['installment_number'] = df['installment_number'].replace({0: 1})

    # ✅ Remove test records
    query_del = """
        SELECT quo_num FROM fininsurance.fin_system_select_plan
        WHERE name IN ('ทดสอบ','test')
        AND type_insure IN ('ประกันรถ', 'ตรอ')
    """
    df_del = pd.read_sql(query_del, source_engine)
    df = df[~df['quotation_num'].isin(df_del['quo_num'])]
    df = df[df['quotation_num'] != 'FQ2505-24999']

    print("\n📊 Cleaning completed")

    return df.replace(['', np.nan], None)

@op
def load_payment_data(df: pd.DataFrame):
    table_name = 'dim_payment_plan'
    pk_column = 'quotation_num'

    # Ensure the unique constraint on quotation_num
    with target_engine.connect() as conn:
        # Check if the unique constraint exists, and add if not
        conn.execute(text(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_quotation_num') THEN
                    ALTER TABLE {table_name} ADD CONSTRAINT unique_quotation_num UNIQUE ({pk_column});
                END IF;
            END $$;
        """))
        conn.commit()

    df = df[~df[pk_column].duplicated(keep='first')].copy()

    # ✅ วันปัจจุบัน (เริ่มต้นเวลา 00:00:00)
    today_str = datetime.now().strftime('%Y-%m-%d')

    # ✅ Load เฉพาะข้อมูลวันนี้จาก PostgreSQL
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(
            f"SELECT * FROM {table_name} WHERE update_at >= '{today_str}'",
            conn
        )

    df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    new_ids = set(df[pk_column]) - set(df_existing[pk_column])
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    common_ids = set(df[pk_column]) & set(df_existing[pk_column])
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))

    exclude_columns = [pk_column, 'payment_plan_id', 'create_at', 'update_at']

    # ✅ คำนวณ column ที่เหมือนกันทั้ง df และ df_existing เท่านั้น
    all_columns = set(df_common_new.columns) & set(df_common_old.columns)
    compare_cols = [
        col for col in all_columns
        if col not in exclude_columns
        and f"{col}_new" in merged.columns
        and f"{col}_old" in merged.columns
    ]

    def is_different(row):
        for col in compare_cols:
            val_new = row.get(f"{col}_new")
            val_old = row.get(f"{col}_old")
            if pd.isna(val_new) and pd.isna(val_old):
                continue
            if val_new != val_old:
                return True
        return False

    # Filter rows that have differences
    df_diff = merged[merged.apply(is_different, axis=1)].copy()

    if not df_diff.empty and compare_cols:
        update_cols = [f"{col}_new" for col in compare_cols]
        all_cols = [pk_column] + update_cols

        # ✅ เช็คให้ชัวร์ว่าคอลัมน์ที่เลือกมีจริง
        existing_cols = [c for c in all_cols if c in df_diff.columns]
        
        if len(existing_cols) > 1:  # ต้องมี pk_column และอย่างน้อย 1 คอลัมน์อื่น
            df_diff_renamed = df_diff.loc[:, existing_cols].copy()
            # เปลี่ยนชื่อ column ให้ตรงกับตารางจริง
            new_col_names = [pk_column] + [col.replace('_new', '') for col in existing_cols if col != pk_column]
            df_diff_renamed.columns = new_col_names
        else:
            df_diff_renamed = pd.DataFrame()
    else:
        df_diff_renamed = pd.DataFrame()

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_diff_renamed)} rows")

    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # Insert only the new records
    if not df_to_insert.empty:
        df_to_insert_valid = df_to_insert[df_to_insert[pk_column].notna()].copy()
        dropped = len(df_to_insert) - len(df_to_insert_valid)
        if dropped > 0:
            print(f"⚠️ Skipped {dropped}")
        if not df_to_insert_valid.empty:
            with target_engine.begin() as conn:
                conn.execute(metadata.insert(), df_to_insert_valid.to_dict(orient='records'))

    # Update only the records where there is a change
    if not df_diff_renamed.empty and compare_cols:
        with target_engine.begin() as conn:
            for record in df_diff_renamed.to_dict(orient='records'):
                stmt = pg_insert(metadata).values(**record)
                update_columns = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns
                    if c.name not in [pk_column, 'payment_plan_id', 'create_at', 'update_at']
                }
                # update_at ให้เป็นเวลาปัจจุบัน
                update_columns['update_at'] = datetime.now()
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")


@job
def dim_payment_plan_etl():
    load_payment_data(clean_payment_data(extract_payment_data()))

# if __name__ == "__main__":
#     df_raw = extract_payment_data()
#     print("✅ Extracted logs:", df_raw.shape)

#     df_clean = clean_payment_data((df_raw))
#     print("✅ Cleaned columns:", df_clean.columns)

#     # output_path = "dim_payment_plan.csv"
#     # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
#     # print(f"💾 Saved to {output_path}")

#     # output_path = "dim_payment_plan.xlsx"
#     # df_clean.to_excel(output_path, index=False, engine='openpyxl')
#     # print(f"💾 Saved to {output_path}")

#     load_payment_data(df_clean)
#     print("🎉 completed! Data upserted to dim_payment_plan.")