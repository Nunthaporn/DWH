from dagster import Definitions, define_asset_job, asset
from .dim_company import dim_company_etl
from .dim_car import dim_car_etl
from .dim_customer import dim_customer_etl
from .dim_agent import dim_agent_etl
from .fact_check_price import fact_check_price_etl
from .fact_installment_payments import fact_installment_payments_etl
from .fact_sales_quotation import fact_sales_quotation_etl
from .fact_commission import fact_commission_etl
from .dim_card_agent import dim_card_agent_etl
from .fact_insurance_motor import fact_insurance_motor_etl
from .update_agent_id import fix_agent_id_on_fact
from .update_car_id import fix_car_id_on_fact
from .update_company_id import update_company_id_job
from .update_payment_plan_id import fix_payment_plan_id_on_fact
from .update_order_type_id import fix_order_type_id_on_fact
from schedules.schedules import dim_company_schedule  
from schedules.schedules import dim_agent_schedule  
from schedules.schedules import dim_car_schedule
from schedules.schedules import dim_customer_schedule
from schedules.schedules import fact_check_price_schedule
from schedules.schedules import fact_installment_payments_schedule
from schedules.schedules import fact_sales_quotation_schedule
from schedules.schedules import fact_commission_schedule
from schedules.schedules import dim_card_agent_schedule
from schedules.schedules import fact_insurance_motor_schedule
from schedules.schedules import update_agent_id_schedule
from schedules.schedules import update_car_id_schedule
from schedules.schedules import update_customer_id_schedule
from schedules.schedules import update_payment_plan_id_schedule
from schedules.schedules import update_order_type_id_schedule

# ✅ ของเก่า (asset)
@asset
def my_simple_asset():
    return "Hello from Dagster!"

assets = [my_simple_asset]

defs = Definitions(
    assets=assets,
    jobs=[dim_company_etl,dim_agent_etl,dim_car_etl,fact_check_price_etl,
          dim_customer_etl,fact_installment_payments_etl,
          fact_sales_quotation_etl,fact_commission_etl,dim_card_agent_etl,fact_insurance_motor_etl,
          fix_agent_id_on_fact,fix_car_id_on_fact,update_company_id_job,
          fix_payment_plan_id_on_fact,fix_order_type_id_on_fact,], 
    schedules=[dim_company_schedule,dim_agent_schedule, dim_car_schedule,
               fact_check_price_schedule,dim_customer_schedule,fact_installment_payments_schedule,
               fact_sales_quotation_schedule, fact_commission_schedule,dim_card_agent_schedule,
               fact_insurance_motor_schedule,update_agent_id_schedule,update_car_id_schedule,update_customer_id_schedule,
               update_payment_plan_id_schedule,update_order_type_id_schedule],              
)

