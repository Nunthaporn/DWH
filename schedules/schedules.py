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

# ทุก 10 นาที ระหว่าง 08:00–22:59
_EVERY_10M_8_TO_22 = "*/10 8-22 * * *"
# ทุก 10 นาที (เริ่มที่นาที 1) ระหว่าง 08:00–22:59
_EVERY_10M_FROM1_8_TO_22 = "1-59/10 8-22 * * *"
_TZ = "Asia/Bangkok"

dim_company_schedule = ScheduleDefinition(
    job=dim_company_etl,
    cron_schedule=_EVERY_10M_8_TO_22,
    execution_timezone=_TZ,
)
dim_agent_schedule = ScheduleDefinition(
    job=dim_agent_etl,
    cron_schedule=_EVERY_10M_8_TO_22,
    execution_timezone=_TZ,
)
dim_car_schedule = ScheduleDefinition(
    job=dim_car_etl,
    cron_schedule=_EVERY_10M_8_TO_22,
    execution_timezone=_TZ,
)
# dim_customer_schedule = ScheduleDefinition(
#     job=dim_customer_etl,
#     cron_schedule=_EVERY_10M_8_TO_22,
#     execution_timezone=_TZ,
# )

fact_check_price_schedule = ScheduleDefinition(
    job=fact_check_price_etl,
    cron_schedule=_EVERY_10M_8_TO_22,
    execution_timezone=_TZ,
)
fact_installment_payments_schedule = ScheduleDefinition(
    job=fact_installment_payments_etl,
    cron_schedule=_EVERY_10M_8_TO_22,
    execution_timezone=_TZ,
)
fact_sales_quotation_schedule = ScheduleDefinition(
    job=fact_sales_quotation_etl,
    cron_schedule=_EVERY_10M_8_TO_22,
    execution_timezone=_TZ,
)
fact_commission_schedule = ScheduleDefinition(
    job=fact_commission_etl,
    cron_schedule=_EVERY_10M_8_TO_22,
    execution_timezone=_TZ,
)
dim_card_agent_schedule = ScheduleDefinition(
    job=dim_card_agent_etl,
    cron_schedule=_EVERY_10M_8_TO_22,
    execution_timezone=_TZ,
)
fact_insurance_motor_schedule = ScheduleDefinition(
    job=fact_insurance_motor_etl,
    cron_schedule=_EVERY_10M_8_TO_22,
    execution_timezone=_TZ,
)

# กลุ่ม update* ให้เริ่มที่นาที 1 แล้วทุก 10 นาที ภายในช่วงเวลาเดียวกัน
update_agent_id_schedule = ScheduleDefinition(
    job=update_agent_id_on_fact,
    cron_schedule=_EVERY_10M_FROM1_8_TO_22,
    execution_timezone=_TZ,
)
update_car_id_schedule = ScheduleDefinition(
    job=update_car_id_on_fact,
    cron_schedule=_EVERY_10M_FROM1_8_TO_22,
    execution_timezone=_TZ,
)
update_company_id_schedule = ScheduleDefinition(
    job=update_company_id_job,
    cron_schedule=_EVERY_10M_FROM1_8_TO_22,
    execution_timezone=_TZ,
)
update_payment_plan_id_schedule = ScheduleDefinition(
    job=update_payment_plan_id_on_fact,
    cron_schedule=_EVERY_10M_FROM1_8_TO_22,
    execution_timezone=_TZ,
)
update_order_type_id_schedule = ScheduleDefinition(
    job=update_order_type_id_on_fact,
    cron_schedule=_EVERY_10M_FROM1_8_TO_22,
    execution_timezone=_TZ,
)