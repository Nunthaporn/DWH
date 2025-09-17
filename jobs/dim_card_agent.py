from dagster import op, job
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, inspect, text, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime

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

    # Normalize ค่าที่เป็นสตริงว่าง/NaN
    df.replace(['NaN', 'nan', 'None'], np.nan, inplace=True)
    df = df.replace(['nan', 'NaN', 'null', ''], np.nan)
    df = df.replace(r'^\s*$', np.nan, regex=True)

    # จัดการคอลัมน์วันที่
    date_columns = [c for c in df.columns if 'date' in c]
    for c in date_columns:
        df[c] = pd.to_datetime(df[c], errors='coerce').where(lambda s: s.notna(), None)

    # ทำความสะอาดคอลัมน์เลขบัตร
    for c in ['id_card_ins', 'id_card_life']:
        if c in df.columns:
            s = df[c].astype(str)
            s = s.replace(['None','nan','NaN'], None)
            s = s.str.replace(r'\D+', '', regex=True).replace('', None)
            df[c] = s

    # # --------- จับคู่ด้วย key ที่ normalize เพื่อกัน space/case ---------
    # # agent_id ที่ตัด space รอบ ๆ (แต่ยังคงรูปเดิมไว้ใน df['agent_id'])
    # df['agent_id_stripped'] = df['agent_id'].astype(str).str.strip()
    # # ใช้ lower สำหรับการตรวจสอบเท่านั้น
    # aid_norm = df['agent_id_stripped'].str.lower()

    # df['has_defect'] = aid_norm.str.endswith('-defect').astype(int)
    # df['base_key']  = aid_norm.str.replace(r'-defect$', '', regex=True)
    # # base_id ที่ “สวย” สำหรับสร้างชื่อผลลัพธ์ (ไม่ lower)
    # df['base_pretty'] = df['agent_id_stripped'].str.replace(r'-defect$', '', regex=True)

    # # คอลัมน์ที่ใช้คำนวณความสมบูรณ์
    # exclude_for_score = {'agent_id', 'agent_id_stripped', 'has_defect', 'base_key', 'base_pretty'}
    # score_cols = [c for c in df.columns if c not in exclude_for_score]

    # def _is_present(x):
    #     if x is None: return False
    #     if isinstance(x, float) and pd.isna(x): return False
    #     if isinstance(x, str) and x.strip() == '': return False
    #     return True

    # df['completeness_score'] = df[score_cols].apply(lambda s: sum(_is_present(v) for v in s.values), axis=1)

    # --- เลือกข้อมูลตามกฎ ---
    out_rows = []
    for base, g in df.groupby('base_key', sort=False):
        has_def = (g['has_defect'] == 1).any()
        has_norm = (g['has_defect'] == 0).any()

        g_sorted = g.sort_values(['completeness_score', 'has_defect'], ascending=[False, False])
        chosen = g_sorted.iloc[0].copy()

        if has_def and has_norm:
            # มีทั้งคู่ → ใช้ข้อมูลที่สมบูรณ์สุด แล้ว "บังคับ" agent_id ให้ลงท้าย -defect
            base_pretty = chosen['base_pretty']  # จาก id ที่ strip แล้ว (รักษาเคส/ฟอร์แมตเดิม)
            chosen['agent_id'] = f"{base_pretty}-defect"
        else:
            # มีชนิดเดียว → ไม่ยุ่ง agent_id
            # แต่ถ้า agent_id มี space รอบ ๆ ให้ใช้ที่ strip แล้ว
            chosen['agent_id'] = chosen['agent_id_stripped']

        out_rows.append(chosen)

    dfc = pd.DataFrame(out_rows).reset_index(drop=True)

    # ลบคอลัมน์ช่วย
    dfc = dfc.drop(columns=['has_defect','completeness_score','base_key','base_pretty','agent_id_stripped'])

    # กันซ้ำ agent_id อีกรอบ (เผื่อ source ข้อมูลซ้ำแถว)
    dfc = dfc.drop_duplicates(subset=['agent_id'], keep='first')

    # แปลง NaN/NaT → None
    dfc = dfc.where(pd.notnull(dfc), None)

    print("\n📊 Cleaning completed (normalize for matching only; force '-defect' ONLY if both exist)")
    # Debug ช่วยตรวจสอบผล
    sample = dfc[dfc['agent_id'].str.contains('113033', na=False)].head(5)
    if not sample.empty:
        print("🔎 sample around 113033:", sample[['agent_id','agent_name']].to_dict('records'))
    return dfc

@op
def load_card_agent_data(df: pd.DataFrame):
    table_name = "dim_agent_card"
    pk_column = "agent_id"

    # 0) ทำความสะอาด NaN -> None
    df = df.where(pd.notnull(df), None)

    # 1) ดึงคีย์ทั้งหมดในตาราง (ง่ายสุดและเร็วพอสำหรับหลักหมื่น/แสน)
    with target_engine.connect() as conn:
        df_existing = pd.read_sql(f"SELECT {pk_column} FROM {table_name}", conn)

    # 2) แบ่ง new vs common
    all_existing = set(df_existing[pk_column])
    new_ids = set(df[pk_column]) - all_existing
    common_ids = set(df[pk_column]) & all_existing

    df_to_insert = df[df[pk_column].isin(new_ids)].copy()
    df_to_upsert = df[df[pk_column].isin(common_ids)].copy()  # สำหรับ UPDATE

    print(f"🆕 Insert candidates: {len(df_to_insert)}")
    print(f"🔄 Update candidates: {len(df_to_upsert)}")

    # 3) โหลดเมทาดาท้า
    metadata_tbl = Table(table_name, MetaData(), autoload_with=target_engine)

    # จะอัปเดตคอลัมน์อะไรบ้าง (ยกเว้น key และ audit/uuid)
    EXCLUDE = {pk_column, "card_ins_uuid", "create_at", "update_at"}
    updateable_cols = [c.name for c in metadata_tbl.columns if c.name not in EXCLUDE]

    # เงื่อนไข WHERE สำหรับ on_conflict_do_update → อัปเดตเฉพาะคอลัมน์ที่ "เปลี่ยนจริง"
    # EXCLUDED.col IS DISTINCT FROM table.col
    # หมายเหตุ: ถ้าต้องการง่ายสุด ตัด where=where_changed ทิ้งได้
    stmt_proto = pg_insert(metadata_tbl)  # ไว้ใช้อ้าง excluded
    where_changed = None
    for c in updateable_cols:
        cond = getattr(stmt_proto.excluded, c).is_distinct_from(getattr(metadata_tbl.c, c))
        where_changed = cond if where_changed is None else (where_changed | cond)

    def chunk_dataframe(dfx: pd.DataFrame, size=1000):
        for i in range(0, len(dfx), size):
            yield dfx.iloc[i:i+size]

    def to_records(dfx: pd.DataFrame) -> list[dict]:
        # แปลงแถวเป็น dict และล้าง NaN/NaT -> None
        out = []
        for _, row in dfx.iterrows():
            rec = {}
            for col, val in row.items():
                if pd.isna(val) or val == pd.NaT or val == "":
                    rec[col] = None
                else:
                    rec[col] = val
            out.append(rec)
        return out

    # 4) INSERT ก้อนใหญ่ด้วย ON CONFLICT DO UPDATE (blind upsert) → ง่ายและเร็ว
    #    หมายเหตุ: เราตั้งค่า create_at/update_at ให้ที่ DB ฝั่ง UPDATE; ส่วน INSERT เราเติมทั้งคู่
    if not df_to_insert.empty:
        with target_engine.begin() as conn:
            for i, batch_df in enumerate(chunk_dataframe(df_to_insert, 5000), start=1):
                recs = to_records(batch_df)
                if not recs:
                    continue

                # เติม audit fields สำหรับ INSERT
                now = datetime.now()
                for r in recs:
                    r.setdefault("create_at", now)
                    r.setdefault("update_at", now)

                stmt = pg_insert(metadata_tbl).values(recs)

                set_map = {c: getattr(stmt.excluded, c) for c in updateable_cols}
                # UPDATE time ให้ DB ใส่เอง (ฟาก update)
                set_map["update_at"] = func.now()

                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=set_map,
                    where=where_changed  # ตัดบรรทัดนี้ทิ้งได้ถ้าต้องการอัปเดตทับไปเลย
                )
                conn.execute(stmt)
                print(f"🟢 Upsert (insert-batch) {i}: {len(recs)} rows")

    # 5) UPDATE (สำหรับ common_ids) — ใช้สูตรเดียวกัน แต่ไม่ต้องเซ็ต create_at
    if not df_to_upsert.empty:
        with target_engine.begin() as conn:
            for i, batch_df in enumerate(chunk_dataframe(df_to_upsert, 1000), start=1):
                recs = to_records(batch_df)
                if not recs:
                    continue

                # อย่าเติม create_at ตอน UPDATE
                for r in recs:
                    if "create_at" in r:
                        del r["create_at"]

                stmt = pg_insert(metadata_tbl).values(recs)
                set_map = {c: getattr(stmt.excluded, c) for c in updateable_cols}
                set_map["update_at"] = func.now()

                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=set_map,
                    where=where_changed
                )
                conn.execute(stmt)
                print(f"🔵 Upsert (update-batch) {i}: {len(recs)} rows")

    print("✅ Insert/update completed.")

    # 6) ตรวจ audit fields ย้อนหลังนิดหน่อย
    to_verify = list(new_ids)[:3] + list(common_ids)[:2]
    if to_verify:
        ids_str = ",".join(f"'{x}'" for x in to_verify)
        q = f"SELECT {pk_column}, create_at, update_at FROM {table_name} WHERE {pk_column} IN ({ids_str}) ORDER BY update_at DESC"
        with target_engine.connect() as conn:
            vf = pd.read_sql(text(q), conn)
            print("🔍 Recent records audit fields:")
            for _, row in vf.iterrows():
                print(f"   {pk_column}: {row[pk_column]}, create_at: {row['create_at']}, update_at: {row['update_at']}")

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