from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, inspect, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ✅ Load environment variables
load_dotenv()

# ✅ DB source: MariaDB
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance"
)

# ✅ DB target: PostgreSQL
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance"
)

@op
def extract_card_agent_data() -> pd.DataFrame:
    query = """
        SELECT 
            ic_ins.cuscode AS agent_id,
            ic_ins.title,
            CONCAT(ic_ins.name, ' ', ic_ins.lastname) AS agent_name,

            ic_ins.card_no AS id_card_ins,
            ic_ins.type AS type_ins,
            ic_ins.revoke_type_code AS revoke_type_ins,
            ic_ins.company AS company_ins,
            CASE 
                WHEN ic_ins.create_date IS NULL OR ic_ins.create_date = '0000-00-00' THEN NULL
                ELSE ic_ins.create_date
            END AS card_issued_date_ins,
            CASE 
                WHEN ic_ins.expire_date IS NULL OR ic_ins.expire_date = '0000-00-00' THEN NULL
                ELSE ic_ins.expire_date
            END AS card_expiry_date_ins,

            ic_life.card_no AS id_card_life,
            ic_life.type AS type_life,
            ic_life.revoke_type_code AS revoke_type_life,
            ic_life.company AS company_life,
            CASE 
                WHEN ic_life.create_date IS NULL OR ic_life.create_date = '0000-00-00' THEN NULL
                ELSE ic_life.create_date
            END AS card_issued_date_life,
            CASE 
                WHEN ic_life.expire_date IS NULL OR ic_life.expire_date = '0000-00-00' THEN NULL
                ELSE ic_life.expire_date
            END AS card_expiry_date_life

        FROM tbl_ins_card ic_ins
        LEFT JOIN tbl_ins_card ic_life
            ON ic_life.cuscode = ic_ins.cuscode AND ic_life.ins_type = 'LIFE'
        WHERE ic_ins.ins_type = 'INS'
            AND ic_ins.cuscode LIKE 'FNG%%'
            AND ic_ins.name NOT LIKE '%%ทดสอบ%%'
            AND ic_ins.name NOT LIKE '%%test%%'
            AND ic_ins.name NOT LIKE '%%เทสระบบ%%'
            AND ic_ins.name NOT LIKE '%%Tes ระบบ%%'
            AND ic_ins.name NOT LIKE '%%ทด่ท%%'
            AND ic_ins.name NOT LIKE '%%ทด สอบ%%'
    """
    df = pd.read_sql(query, source_engine)

    print("📦 df:", df.shape)

    return df

@op
def clean_card_agent_data(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()

    # ✅ Replace string 'NaN', 'nan', 'None' with actual np.nan
    df.replace(['NaN', 'nan', 'None'], np.nan, inplace=True)

    # ✅ Handle date columns properly
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    for col in date_columns:
        # convert to datetime and force errors to NaT
        df[col] = pd.to_datetime(df[col], errors='coerce')
        # convert NaT to None for PostgreSQL compatibility
        df[col] = df[col].where(pd.notnull(df[col]), None)

    # ✅ Clean up ID card columns
    id_card_columns = ['id_card_ins', 'id_card_life']
    for col in id_card_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['None', 'nan', 'NaN'], None)
            df[col] = df[col].str.replace(r'\D+', '', regex=True)  # remove non-digit
            df[col] = df[col].replace('', None)

    # ✅ Convert all NaN values to None for PostgreSQL compatibility
    df = df.where(pd.notnull(df), None)

    print("\n📊 Cleaning completed")
    return df

@op
def load_card_agent_data(df: pd.DataFrame):
    table_name = 'dim_card_agent'
    pk_column = 'agent_id'

    # ✅ กรอง agent_id ซ้ำจาก DataFrame ใหม่
    df = df[~df[pk_column].duplicated(keep='first')].copy()
    
    # ✅ กรองข้อมูลที่ agent_id ไม่เป็น None
    df = df[df[pk_column].notna()].copy()
    
    if df.empty:
        print("⚠️ No valid data to process")
        return

    # ✅ โหลดเฉพาะ agent_id ที่มีอยู่ในข้อมูลใหม่ (ไม่โหลดทั้งหมด)
    agent_ids = df[pk_column].tolist()
    
    if not agent_ids:
        df_existing = pd.DataFrame()
    else:
        # แปลง agent_ids เป็น tuple เพื่อให้ SQLAlchemy รับได้
        agent_ids_tuple = tuple(agent_ids)
        
        placeholders = ','.join(['%s'] * len(agent_ids))
        query_existing = f"""
            SELECT * FROM {table_name} 
            WHERE {pk_column} IN ({placeholders})
        """

        with target_engine.connect() as conn:
            df_existing = pd.read_sql(
                text(query_existing), 
                conn, 
                params=agent_ids
            )

    print(f"📊 New data: {len(df)} rows")
    print(f"📊 Existing data found: {len(df_existing)} rows")

    # ✅ กรอง agent_id ซ้ำจากข้อมูลเก่า
    if not df_existing.empty:
        df_existing = df_existing[~df_existing[pk_column].duplicated(keep='first')].copy()

    # ✅ Identify agent_id ใหม่ (ไม่มีใน DB)
    existing_ids = set(df_existing[pk_column]) if not df_existing.empty else set()
    new_ids = set(df[pk_column]) - existing_ids
    df_to_insert = df[df[pk_column].isin(new_ids)].copy()

    # ✅ Identify agent_id ที่มีอยู่แล้ว
    common_ids = set(df[pk_column]) & existing_ids
    df_common_new = df[df[pk_column].isin(common_ids)].copy()
    df_common_old = df_existing[df_existing[pk_column].isin(common_ids)].copy()

    # ✅ ระบุคอลัมน์ที่ใช้เปรียบเทียบ (ยกเว้น key และ audit fields)
    exclude_columns = [pk_column, 'card_ins_uuid', 'create_at', 'update_at']
    compare_cols = [
        col for col in df.columns
        if col not in exclude_columns
    ]

    # ✅ เปรียบเทียบข้อมูลแบบ Vectorized (เร็วกว่า apply)
    df_to_update = pd.DataFrame()
    if not df_common_new.empty and not df_common_old.empty:
        # Merge ด้วย suffix (_new, _old)
        merged = df_common_new.merge(df_common_old, on=pk_column, suffixes=('_new', '_old'))
        
        # ตรวจสอบว่าคอลัมน์ที่มีอยู่ใน merged DataFrame
        available_cols = []
        for col in compare_cols:
            if f"{col}_new" in merged.columns and f"{col}_old" in merged.columns:
                available_cols.append(col)
        
        if available_cols:
            # สร้าง boolean mask สำหรับแถวที่มีความแตกต่าง
            diff_mask = pd.Series(False, index=merged.index)
            
            for col in available_cols:
                col_new = f"{col}_new"
                col_old = f"{col}_old"
                
                # เปรียบเทียบแบบ vectorized
                new_vals = merged[col_new]
                old_vals = merged[col_old]
                
                # จัดการ NaN values
                both_nan = (pd.isna(new_vals) & pd.isna(old_vals))
                different = (new_vals != old_vals) & ~both_nan
                
                diff_mask |= different
            
            # กรองแถวที่มีความแตกต่าง
            df_diff = merged[diff_mask].copy()
            
            if not df_diff.empty:
                # เตรียมข้อมูลสำหรับ update
                update_cols = [f"{col}_new" for col in available_cols]
                all_cols = [pk_column] + update_cols
                
                # ตรวจสอบว่าคอลัมน์ทั้งหมดมีอยู่ใน df_diff
                existing_cols = [col for col in all_cols if col in df_diff.columns]
                
                if len(existing_cols) > 1:
                    df_to_update = df_diff[existing_cols].copy()
                    # เปลี่ยนชื่อ column ให้ตรงกับตารางจริง
                    new_col_names = [pk_column] + [col.replace('_new', '') for col in existing_cols if col != pk_column]
                    df_to_update.columns = new_col_names

    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_to_update)} rows")

    # ✅ Load table metadata
    metadata = Table(table_name, MetaData(), autoload_with=target_engine)

    # ✅ Insert (Batch operation)
    if not df_to_insert.empty:
        # แปลง DataFrame เป็น records
        records = []
        for _, row in df_to_insert.iterrows():
            record = {}
            for col, value in row.items():
                if pd.isna(value) or value == pd.NaT or value == '':
                    record[col] = None
                else:
                    record[col] = value
            records.append(record)
        
        # Insert แบบ batch
        with target_engine.begin() as conn:
            conn.execute(metadata.insert(), records)

    # ✅ Update (Batch operation)
    if not df_to_update.empty:
        # แปลง DataFrame เป็น records
        records = []
        for _, row in df_to_update.iterrows():
            record = {}
            for col, value in row.items():
                if pd.isna(value) or value == pd.NaT or value == '':
                    record[col] = None
                else:
                    record[col] = value
            records.append(record)
        
        # Update แบบ batch
        with target_engine.begin() as conn:
            for record in records:
                stmt = pg_insert(metadata).values(**record)
                update_columns = {
                    c.name: stmt.excluded[c.name]
                    for c in metadata.columns
                    if c.name != pk_column
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_columns
                )
                conn.execute(stmt)

    print("✅ Insert/update completed.")

@job
def dim_card_agent_etl():
    load_card_agent_data(clean_card_agent_data(extract_card_agent_data()))

# if __name__ == "__main__":
#     df_raw = extract_card_agent_data()

#     df_clean = clean_card_agent_data((df_raw))
#     print("✅ Cleaned columns:", df_clean.columns)

#     # output_path = "dim_card_agent.csv"
#     # df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
#     # print(f"💾 Saved to {output_path}")

#     # output_path = "dim_card_agent.xlsx"
#     # df_clean.to_excel(output_path, index=False, engine='openpyxl')
#     # print(f"💾 Saved to {output_path}")

#     load_card_agent_data(df_clean)
#     print("🎉 completed! Data upserted to dim_card_agent.")