from dagster import Definitions, define_asset_job, asset
from .dim_company import dim_company_etl
from .dim_sales import dim_sales_etl
from .dim_car import dim_car_etl
from .fact_check_price import fact_check_price_etl
from .dim_customer import dim_customer_etl
from .dim_agent import dim_agent_etl
from .dim_order_type import dim_order_type_etl
from .dim_payment_plan import dim_payment_plan_etl
from schedules.schedules import dim_company_schedule  # ✅ เพิ่ม import schedule
from schedules.schedules import dim_sales_schedule  # ✅ เพิ่ม import schedule
from schedules.schedules import dim_agent_schedule  # ✅ เพิ่ม import schedule
from schedules.schedules import dim_car_schedule
from schedules.schedules import fact_check_price_schedule
from schedules.schedules import dim_customer_schedule
from schedules.schedules import dim_order_type_schedule
from schedules.schedules import dim_payment_plan_schedule

# ✅ ของเก่า (asset)
@asset
def my_simple_asset():
    return "Hello from Dagster!"

assets = [my_simple_asset]


# ✅ รวม definitions ทั้งหมด
defs = Definitions(
    assets=assets,
    jobs=[dim_company_etl, dim_sales_etl,dim_agent_etl,dim_car_etl,fact_check_price_etl,dim_customer_etl,dim_order_type_etl,dim_payment_plan_etl], 
    schedules=[dim_company_schedule,dim_sales_schedule,dim_agent_schedule, dim_car_schedule,fact_check_price_schedule,dim_customer_schedule,dim_order_type_schedule,dim_payment_plan_schedule],              
)
