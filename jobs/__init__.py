from dagster import Definitions, define_asset_job, asset
from .dim_company import dim_company_etl
from .dim_sales import dim_sales_etl
from .dim_car import dim_car_etl
from .dim_customer import dim_customer_etl
from .dim_agent import dim_agent_etl
from .dim_order_type import dim_order_type_etl
from .dim_payment_plan import dim_payment_plan_etl
from .fact_check_price import fact_check_price_etl
from .fact_installment_payments import fact_installment_payments_etl
from .fact_sales_quotation import fact_sales_quotation_etl
from .fact_commission import fact_commission_etl
from .fact_card_ins import fact_card_ins_etl
from .fact_insurance_motor import fact_insurance_motor_etl
from .update_agent_id import update_fact_sales_quotation_agent_id
from .update_car_id import update_fact_sales_quotation_car_id
from .update_customer_id import update_fact_sales_quotation_customer_id
from .update_payment_plan_id import update_fact_sales_quotation_payment_plan_id
from .update_order_type_id import update_fact_sales_quotation_order_type_id
from .update_sales_id import update_fact_sales_quotation_sales_id
from schedules.schedules import dim_company_schedule  
from schedules.schedules import dim_sales_schedule  
from schedules.schedules import dim_agent_schedule  
from schedules.schedules import dim_car_schedule
from schedules.schedules import dim_customer_schedule
from schedules.schedules import dim_order_type_schedule
from schedules.schedules import dim_payment_plan_schedule
from schedules.schedules import fact_check_price_schedule
from schedules.schedules import fact_installment_payments_schedule
from schedules.schedules import fact_sales_quotation_schedule
from schedules.schedules import fact_commission_schedule
from schedules.schedules import fact_card_ins_schedule
from schedules.schedules import fact_insurance_motor_schedule
from schedules.schedules import update_agent_id_schedule
from schedules.schedules import update_car_id_schedule
from schedules.schedules import update_customer_id_schedule
from schedules.schedules import update_payment_plan_id_schedule
from schedules.schedules import update_order_type_id_schedule
from schedules.schedules import update_sales_id_schedule

# ✅ ของเก่า (asset)
@asset
def my_simple_asset():
    return "Hello from Dagster!"

assets = [my_simple_asset]

defs = Definitions(
    assets=assets,
    jobs=[dim_company_etl, dim_sales_etl,dim_agent_etl,dim_car_etl,fact_check_price_etl,
          dim_customer_etl,dim_order_type_etl,dim_payment_plan_etl,fact_installment_payments_etl,
          fact_sales_quotation_etl,fact_commission_etl,fact_card_ins_etl,fact_insurance_motor_etl,
          update_fact_sales_quotation_agent_id,update_fact_sales_quotation_car_id,update_fact_sales_quotation_customer_id,
          update_fact_sales_quotation_payment_plan_id,update_fact_sales_quotation_order_type_id,update_fact_sales_quotation_sales_id], 
    schedules=[dim_company_schedule,dim_sales_schedule,dim_agent_schedule, dim_car_schedule,
               fact_check_price_schedule,dim_customer_schedule,dim_order_type_schedule,
               dim_payment_plan_schedule,fact_installment_payments_schedule,
               fact_sales_quotation_schedule, fact_commission_schedule,fact_card_ins_schedule,
               fact_insurance_motor_schedule,update_agent_id_schedule,update_car_id_schedule,update_customer_id_schedule,
               update_payment_plan_id_schedule,update_order_type_id_schedule, update_sales_id_schedule],              
)

