{{ config( materialized='table') }}

select
    * exclude rw
from
    (select
        msisdn,
        imei_tac,
        brand_name,
        model_name,
        os_name,
        os_vendor,
        row_number() over(partition by msisdn order by os_name, os_vendor) as rw
    from telecom_database.raw.device1
    ) as a
where
    rw = 1