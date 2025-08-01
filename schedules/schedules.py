from dagster import ScheduleDefinition
from jobs.dim_company import dim_company_etl
from jobs.dim_sales import dim_sales_etl
from jobs.dim_car import dim_car_etl
from jobs.dim_customer import dim_customer_etl
from jobs.dim_agent import dim_agent_etl
from jobs.dim_order_type import dim_order_type_etl
from jobs.dim_payment_plan import dim_payment_plan_etl
from jobs.fact_check_price import fact_check_price_etl
from jobs.fact_installment_payments import fact_installment_payments_etl
from jobs.fact_sales_quotation import fact_sales_quotation_etl
from jobs.fact_commission import fact_commission_etl
from jobs.dim_card_agent import dim_card_agent_etl
from jobs.fact_insurance_motor import fact_insurance_motor_etl
from jobs.update_agent_id import update_fact_sales_quotation_agent_id
from jobs.update_car_id import update_fact_sales_quotation_car_id
from jobs.update_customer_id import update_fact_sales_quotation_customer_id
from jobs.update_payment_plan_id import update_fact_sales_quotation_payment_plan_id
from jobs.update_order_type_id import update_fact_sales_quotation_order_type_id
from jobs.update_sales_id import update_fact_sales_quotation_sales_id

dim_company_schedule = ScheduleDefinition(
    job=dim_company_etl,
    cron_schedule="0 * * * *", 
)
dim_sales_schedule = ScheduleDefinition(
    job=dim_sales_etl,
    cron_schedule="0 * * * *", 
)
dim_agent_schedule = ScheduleDefinition(
    job=dim_agent_etl,
    cron_schedule="0 * * * *", 
)
dim_car_schedule = ScheduleDefinition(
    job=dim_car_etl,
    cron_schedule="0 * * * *", 
)
dim_customer_schedule = ScheduleDefinition(
    job=dim_customer_etl,
    cron_schedule="0 * * * *", 
)
dim_order_type_schedule = ScheduleDefinition(
    job=dim_order_type_etl,
    cron_schedule="0 * * * *", 
)
dim_payment_plan_schedule = ScheduleDefinition(
    job=dim_payment_plan_etl,
    cron_schedule="0 * * * *", 
)
fact_check_price_schedule = ScheduleDefinition(
    job=fact_check_price_etl,
    cron_schedule="0 * * * *", 
)
fact_installment_payments_schedule = ScheduleDefinition(
    job=fact_installment_payments_etl,
    cron_schedule="0 */2 * * *", 
)
fact_sales_quotation_schedule = ScheduleDefinition(
    job=fact_sales_quotation_etl,
    cron_schedule="0 * * * *", 
)
fact_commission_schedule = ScheduleDefinition(
    job=fact_commission_etl,
    cron_schedule="0 * * * *", 
)
dim_card_agent_schedule = ScheduleDefinition(
    job=dim_card_agent_etl,
    cron_schedule="0 * * * *", 
)
fact_insurance_motor_schedule = ScheduleDefinition(
    job=fact_insurance_motor_etl,
    cron_schedule="0 * * * *", 
)
update_agent_id_schedule = ScheduleDefinition(
    job=update_fact_sales_quotation_agent_id,
    cron_schedule="0 * * * *", 
)
update_car_id_schedule = ScheduleDefinition(
    job=update_fact_sales_quotation_car_id,
    cron_schedule="0 * * * *", 
)
update_customer_id_schedule = ScheduleDefinition(
    job=update_fact_sales_quotation_customer_id,
    cron_schedule="0 * * * *", 
)
update_payment_plan_id_schedule = ScheduleDefinition(
    job=update_fact_sales_quotation_payment_plan_id,
    cron_schedule="0 * * * *", 
)
update_order_type_id_schedule = ScheduleDefinition(
    job=update_fact_sales_quotation_order_type_id,
    cron_schedule="0 * * * *", 
)
update_sales_id_schedule = ScheduleDefinition(
    job=update_fact_sales_quotation_sales_id,
    cron_schedule="0 * * * *", 
)