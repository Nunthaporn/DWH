from dagster import ScheduleDefinition
from jobs.dim_company import dim_company_etl
from jobs.dim_sales import dim_sales_etl
from jobs.dim_car import dim_car_etl
from jobs.fact_check_price import fact_check_price_etl
from jobs.dim_customer import dim_customer_etl
from jobs.dim_agent import dim_agent_etl

dim_company_schedule = ScheduleDefinition(
    job=dim_company_etl,
    cron_schedule="0 * * * *",  # ทุกชั่วโมง
    
)
dim_sales_schedule = ScheduleDefinition(
    job=dim_sales_etl,
    cron_schedule="0 * * * *",  # ทุกชั่วโมง
)
dim_agent_schedule = ScheduleDefinition(
    job=dim_agent_etl,
    cron_schedule="0 * * * *",  # ทุกชั่วโมง
)
dim_car_schedule = ScheduleDefinition(
    job=dim_car_etl,
    cron_schedule="0 * * * *", 
)
fact_check_price_schedule = ScheduleDefinition(
    job=fact_check_price_etl,
    cron_schedule="0 * * * *", 
)
dim_customer_schedule = ScheduleDefinition(
    job=dim_customer_etl,
    cron_schedule="0 * * * *", 
)