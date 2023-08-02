{{ config( materialized='table', schema= 'Final') }}

select os_name,
sum(revenue_usd) Revenue  from
{{ref('Curated')}}
group by 1