{{ config( materialized='table', schema= 'Final') }}

with active_device as (
select 'id' as id,count(distinct imei_tac ) active_devices from {{ref('Curated')}} where system_status='ACTIVE'
), all_device as (
select 'id' as id,count(distinct imei_tac ) all_devices from {{ref('Curated')}}
) , c as (
    select active_devices, all_devices from active_device j join all_device j2 where j.id = j2.id
) select * from c