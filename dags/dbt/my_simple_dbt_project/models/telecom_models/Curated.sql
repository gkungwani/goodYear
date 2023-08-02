{{ config( materialized='table', schema= 'Curated') }}

select
    rev.msisdn,
    imei_tac,
    gender,
    FLOOR((YEAR(CURRENT_DATE()) - year_of_birth)) AS age,
    mobile_type,
    value_segment,
    system_status,
    brand_name,
    model_name,
    os_name,
    os_vendor,
    week_number,
    sum(revenue_usd) as revenue_usd
from
    {{ref('Cleansed_revenue1')}} as rev

left join
    {{ref('Cleansed_crm1')}} as crm
on
    rev.msisdn = crm.msisdn

left join
    {{ref('Cleansed_device1')}} as device
on
    rev.msisdn = device.msisdn

group by
    1,2,3,4,5,6,7,8,9,10,11,12
order by
    1,2,3,4,5,6,7,8,9,10,11,12
