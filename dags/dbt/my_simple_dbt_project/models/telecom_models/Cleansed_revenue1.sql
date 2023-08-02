{{ config( materialized='table') }}

select
    msisdn,
    week_number,
    revenue_usd
from telecom_database.raw.rev1
