from dagster import op, job
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table

# ============== ENV & DB CONNECTIONS ==============
load_dotenv()

# MariaDB (source: fininsurance_task)
source_engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/fininsurance_task",
    pool_pre_ping=True, pool_recycle=3600
)

# PostgreSQL (target)
target_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER_test')}:{os.getenv('DB_PASSWORD_test')}@"
    f"{os.getenv('DB_HOST_test')}:{os.getenv('DB_PORT_test')}/fininsurance",
    pool_pre_ping=True, pool_recycle=3600,
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c statement_timeout=300000"
    }
)

# ============== OPS ==============

@op
def extract_company_sources():
    """ดึงข้อมูลจาก fin_order (source), fact_sales_quotation และ dim_company (target)."""
    with source_engine.begin() as sconn:
        df_order = pd.read_sql(text("SELECT quo_num, newinsurance FROM fin_order"), sconn)

    with target_engine.begin() as tconn:
        df_fact = pd.read_sql(text("SELECT quotation_num FROM fact_sales_quotation"), tconn)
        df_dim  = pd.read_sql(text("SELECT company_id, company_name FROM dim_company"), tconn)

    # ตั้งชื่อคอลัมน์ให้ตรง
    df_order = df_order.rename(columns={"quo_num": "quotation_num", "newinsurance": "company_name"})
    return df_order, df_fact, df_dim


@op
def build_company_temp(dfs) -> pd.DataFrame:

    df_order, df_fact, df_dim = dfs

    # clean เบื้องต้น
    for c in ("quotation_num", "company_name"):
        if c in df_order.columns:
            df_order[c] = df_order[c].astype(str).str.strip()

    # ครอบคลุม quotation ใน fact ทั้งหมด
    df_m1 = pd.merge(df_order, df_fact, on="quotation_num", how="right")

    # แม็พชื่อบริษัทเป็น id
    df_map = pd.merge(df_m1, df_dim, on="company_name", how="left")

    # เลือกคอลัมน์ที่ต้องใช้ และทำความสะอาด
    temp = df_map[["quotation_num", "company_id"]].copy()

    # แปลงค่าที่สื่อความว่างให้เป็น NA แล้วตัดแถว company_id ว่าง (กันอัปเดต NULL)
    temp = temp.replace(['None', 'none', 'nan', 'NaN', 'NaT', ''], pd.NA)
    temp["quotation_num"] = temp["quotation_num"].astype("string").str.strip()
    # เก็บเฉพาะแถวที่มี company_id จริง
    temp = temp[temp["company_id"].notna()].copy()

    # กันซ้ำในกรณี 1 quotation อาจแม็พได้หลายแถว
    temp = temp.drop_duplicates(subset=["quotation_num"], keep="first")

    return temp


@op
def update_fact_company_id(df_temp: pd.DataFrame):
    if df_temp.empty:
        print("⚠️ No rows to update.")
        return

    temp_table = "dim_company_temp"

    with target_engine.begin() as conn:
        # ✅ ใช้ SQLAlchemy Connection (conn) ไม่ใช่ conn.connection
        df_temp.to_sql(
            temp_table,
            con=conn,                 # <<< ตรงนี้คือจุดแก้
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=20000
        )
        print(f"✅ loaded temp rows: {len(df_temp):,}")

        update_sql = text(f"""
            UPDATE fact_sales_quotation fsq
            SET company_id = dc.company_id
            FROM {temp_table} dc
            WHERE fsq.quotation_num = dc.quotation_num
        """)
        res = conn.execute(update_sql)
        print(f"🔄 updated rows: {res.rowcount:,}")

        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        print("🗑️ dropped temp table")


# ============== DAGSTER JOB ==============

@job
def update_company_id_job():
    update_fact_company_id(build_company_temp(extract_company_sources()))


# ============== OPTIONAL LOCAL RUN ==============
if __name__ == "__main__":
    dfs = extract_company_sources()
    temp = build_company_temp(dfs)
    update_fact_company_id(temp)
    print("🎉 completed! Updated company_id in fact_sales_quotation.")
