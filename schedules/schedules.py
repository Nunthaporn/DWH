from dagster import ScheduleDefinition
from jobs.dim_company import dim_company_etl
from jobs.dim_sales import dim_sales_etl
from jobs.dim_agent import dim_agent_etl
from jobs.dim_car import dim_car_etl


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
    cron_schedule="0 * * * *",  # ทุกชั่วโมง
)