{{ config( materialized='table', schema= 'Final') }}

with active_customer as (
select 'id' as id,count(distinct msisdn ) active_customers from {{ref('Curated')}} where system_status='ACTIVE'
), all_customer as (
select 'id' as id,count(distinct msisdn ) all_customers from {{ref('Curated')}}
) , c as (
    select active_customers, all_customers from active_customer j join all_customer j2 where j.id = j2.id
) select * from c