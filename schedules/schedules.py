from dagster import ScheduleDefinition
from jobs.dim_company import dim_company_etl
from jobs.dim_car import dim_car_etl
# from jobs.dim_customer import dim_customer_etl
from jobs.dim_agent import dim_agent_etl
from jobs.fact_check_price import fact_check_price_etl
from jobs.fact_installment_payments import fact_installment_payments_etl
from jobs.fact_sales_quotation import fact_sales_quotation_etl
from jobs.fact_commission import fact_commission_etl
from jobs.dim_card_agent import dim_card_agent_etl
from jobs.fact_insurance_motor import fact_insurance_motor_etl
from jobs.update_agent_id import update_agent_id_on_fact
from jobs.update_car_id import update_car_id_on_fact
from jobs.update_company_id import update_company_id_job
from jobs.update_payment_plan_id import update_payment_plan_id_on_fact
from jobs.update_order_type_id import update_order_type_id_on_fact

dim_company_schedule = ScheduleDefinition(
    job=dim_company_etl,
    cron_schedule="*/15 * * * *", 
)
dim_agent_schedule = ScheduleDefinition(
    job=dim_agent_etl,
    cron_schedule="*/15 * * * *", 
)
dim_car_schedule = ScheduleDefinition(
    job=dim_car_etl,
    cron_schedule="*/15 * * * *", 
)
# dim_customer_schedule = ScheduleDefinition(
#     job=dim_customer_etl,
#     cron_schedule="*/15 * * * *", 
# )
fact_check_price_schedule = ScheduleDefinition(
    job=fact_check_price_etl,
    cron_schedule="*/15 * * * *", 
)
fact_installment_payments_schedule = ScheduleDefinition(
    job=fact_installment_payments_etl,
    cron_schedule="*/15 * * * *", 
)
fact_sales_quotation_schedule = ScheduleDefinition(
    job=fact_sales_quotation_etl,
    cron_schedule="*/15 * * * *", 
)
fact_commission_schedule = ScheduleDefinition(
    job=fact_commission_etl,
    cron_schedule="*/15 * * * *", 
)
dim_card_agent_schedule = ScheduleDefinition(
    job=dim_card_agent_etl,
    cron_schedule="*/15 * * * *", 
)
fact_insurance_motor_schedule = ScheduleDefinition(
    job=fact_insurance_motor_etl,
    cron_schedule="*/15 * * * *", 
)
update_agent_id_schedule = ScheduleDefinition(
    job=update_agent_id_on_fact,
    cron_schedule="1/15 * * * *", 
)
update_car_id_schedule = ScheduleDefinition(
    job=update_car_id_on_fact,
    cron_schedule="1/15 * * * *", 
)
update_company_id_schedule = ScheduleDefinition(
    job=update_company_id_job,
    cron_schedule="1/15 * * * *", 
)
update_payment_plan_id_schedule = ScheduleDefinition(
    job=update_payment_plan_id_on_fact,
    cron_schedule="1/15 * * * *", 
)
update_order_type_id_schedule = ScheduleDefinition(
    job=update_order_type_id_on_fact,
    cron_schedule="1/15 * * * *", 
)
