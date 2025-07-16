from dagster import Definitions, define_asset_job, asset
from .dim_company import dim_company_etl
from .dim_sales import dim_sales_etl
from .dim_agent import dim_agent_etl
from schedules.schedules import dim_company_schedule  # ✅ เพิ่ม import schedule
from schedules.schedules import dim_sales_schedule  # ✅ เพิ่ม import schedule
from schedules.schedules import dim_agent_schedule  # ✅ เพิ่ม import schedule

# ✅ ของเก่า (asset)
@asset
def my_simple_asset():
    return "Hello from Dagster!"

assets = [my_simple_asset]


# ✅ รวม definitions ทั้งหมด
defs = Definitions(
    assets=assets,
    jobs=[dim_company_etl, dim_sales_etl,dim_agent_etl], 
    schedules=[dim_company_schedule,dim_sales_schedule,dim_agent_schedule ],                         # schedule ที่มี
)
