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

    # ✅ ใช้ temporary table เพื่อเพิ่มประสิทธิภาพ
    temp_table_name = f'temp_{table_name}_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}'
    
    # ✅ สร้าง temporary table และ insert ข้อมูลใหม่
    with target_engine.begin() as conn:
        # สร้าง temporary table
        create_temp_table_sql = f"""
            CREATE TEMP TABLE {temp_table_name} AS 
            SELECT * FROM {table_name} WHERE 1=0
        """
        conn.execute(text(create_temp_table_sql))
        
        # Insert ข้อมูลใหม่ลง temporary table (กรอง duplicate ก่อน)
        if not df.empty:
            # แปลง DataFrame เป็น records
            records = []
            current_time = pd.Timestamp.now()
            seen_agent_ids = set()  # ใช้เพื่อตรวจสอบ duplicate
            
            for _, row in df.iterrows():
                agent_id = row[pk_column]
                
                # ข้ามถ้า agent_id ซ้ำ
                if agent_id in seen_agent_ids:
                    continue
                seen_agent_ids.add(agent_id)
                
                record = {}
                for col, value in row.items():
                    if pd.isna(value) or value == pd.NaT or value == '':
                        record[col] = None
                    else:
                        record[col] = value
                # ✅ ตั้งค่า audit fields สำหรับ insert
                record['create_at'] = current_time
                record['update_at'] = current_time
                records.append(record)
            
            # Insert แบบ batch ลง temporary table (ไม่ใช่ตารางหลัก)
            if records:
                # ใช้ temporary table metadata แทน
                temp_metadata = Table(temp_table_name, MetaData(), autoload_with=target_engine)
                conn.execute(temp_metadata.insert(), records)

    # ✅ ใช้ SQL เพื่อหา records ที่ต้อง insert และ update
    with target_engine.connect() as conn:
        # หา records ใหม่ (ไม่มีในตารางหลัก)
        insert_query = f"""
            SELECT t.* 
            FROM {temp_table_name} t
            LEFT JOIN {table_name} m ON t.{pk_column} = m.{pk_column}
            WHERE m.{pk_column} IS NULL
        """
        df_to_insert = pd.read_sql(text(insert_query), conn)
        
        # หา records ที่ต้อง update (มีในตารางหลักแต่ข้อมูลต่างกัน)
        update_query = f"""
            SELECT t.*, m.{pk_column} as existing_{pk_column}
            FROM {temp_table_name} t
            INNER JOIN {table_name} m ON t.{pk_column} = m.{pk_column}
            WHERE (
                COALESCE(t.title, '') != COALESCE(m.title, '') OR
                COALESCE(t.agent_name, '') != COALESCE(m.agent_name, '') OR
                COALESCE(t.id_card_ins, '') != COALESCE(m.id_card_ins, '') OR
                COALESCE(t.type_ins, '') != COALESCE(m.type_ins, '') OR
                COALESCE(t.revoke_type_ins, '') != COALESCE(m.revoke_type_ins, '') OR
                COALESCE(t.company_ins, '') != COALESCE(m.company_ins, '') OR
                COALESCE(t.card_issued_date_ins::text, '') != COALESCE(m.card_issued_date_ins::text, '') OR
                COALESCE(t.card_expiry_date_ins::text, '') != COALESCE(m.card_expiry_date_ins::text, '') OR
                COALESCE(t.id_card_life, '') != COALESCE(m.id_card_life, '') OR
                COALESCE(t.type_life, '') != COALESCE(m.type_life, '') OR
                COALESCE(t.revoke_type_life, '') != COALESCE(m.revoke_type_life, '') OR
                COALESCE(t.company_life, '') != COALESCE(m.company_life, '') OR
                COALESCE(t.card_issued_date_life::text, '') != COALESCE(m.card_issued_date_life::text, '') OR
                COALESCE(t.card_expiry_date_life::text, '') != COALESCE(m.card_expiry_date_life::text, '')
            )
        """
        df_to_update = pd.read_sql(text(update_query), conn)

    print(f"📊 New data: {len(df)} rows")
    print(f"🆕 Insert: {len(df_to_insert)} rows")
    print(f"🔄 Update: {len(df_to_update)} rows")

    # ✅ Insert records ใหม่
    if not df_to_insert.empty:
        with target_engine.begin() as conn:
            # ลบคอลัมน์ existing_agent_id ออกถ้ามี
            if 'existing_agent_id' in df_to_insert.columns:
                df_to_insert = df_to_insert.drop('existing_agent_id', axis=1)
            
            # แปลงเป็น records
            records = df_to_insert.to_dict('records')
            metadata = Table(table_name, MetaData(), autoload_with=target_engine)
            conn.execute(metadata.insert(), records)

    # ✅ Update records ที่มีอยู่แล้ว - ใช้ batch UPDATE
    if not df_to_update.empty:
        with target_engine.begin() as conn:
            # ลบคอลัมน์ existing_agent_id ออก
            if 'existing_agent_id' in df_to_update.columns:
                df_to_update = df_to_update.drop('existing_agent_id', axis=1)
            
            # ใช้ batch UPDATE แทนการทำทีละแถว
            update_columns = [
                'title', 'agent_name', 'id_card_ins', 'type_ins', 'revoke_type_ins', 
                'company_ins', 'card_issued_date_ins', 'card_expiry_date_ins',
                'id_card_life', 'type_life', 'revoke_type_life', 'company_life',
                'card_issued_date_life', 'card_expiry_date_life', 'update_at'
            ]
            
            # สร้าง batch UPDATE statement
            set_clause = ', '.join([f"{col} = t.{col}" for col in update_columns if col != 'update_at'])
            set_clause += ", update_at = NOW()"
            
            # ใช้ CASE WHEN สำหรับ batch update
            agent_ids = df_to_update[pk_column].tolist()
            if agent_ids:
                # แบ่งเป็น chunks เพื่อหลีกเลี่ยง query ที่ยาวเกินไป
                chunk_size = 1000
                for i in range(0, len(agent_ids), chunk_size):
                    chunk_ids = agent_ids[i:i+chunk_size]
                    ids_str = ','.join([f"'{id}'" for id in chunk_ids])
                    
                    update_sql = f"""
                        UPDATE {table_name} m
                        SET {set_clause}
                        FROM {temp_table_name} t
                        WHERE m.{pk_column} = t.{pk_column}
                        AND m.{pk_column} IN ({ids_str})
                    """
                    conn.execute(text(update_sql))

    # ✅ ลบ temporary table
    with target_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))

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