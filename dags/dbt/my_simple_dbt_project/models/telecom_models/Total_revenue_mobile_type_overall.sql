{{ config( materialized='table', schema= 'Final') }}

select mobile_type,
sum(revenue_usd) Revenue  from
{{ref('Curated')}}
group by 1