{{ config( materialized='table', schema= 'Final') }}

with active_customer as (
select week_number, count(distinct msisdn ) active_customers from {{ref('Curated')}} where system_status='ACTIVE' group by 1 order by 1
), all_customer as (
select week_number, count(distinct msisdn ) all_customers from {{ref('Curated')}} group by 1 order by 1
) , c as (
    select j.week_number,active_customers, all_customers from active_customer j join all_customer j2 where j.week_number = j2.week_number
) select * from c