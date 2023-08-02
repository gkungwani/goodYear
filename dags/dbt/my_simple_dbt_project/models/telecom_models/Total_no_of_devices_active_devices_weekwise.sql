{{ config( materialized='table', schema= 'Final') }}

with active_device as (
select week_number, count(distinct imei_tac ) active_devices from {{ref('Curated')}} where system_status='ACTIVE' group by 1 order by 1
), all_device as (
select week_number, count(distinct imei_tac ) all_devices from {{ref('Curated')}} group by 1 order by 1
) , c as (
    select j.week_number,active_devices, all_devices from active_device j join all_device j2 where j.week_number = j2.week_number
) select * from c