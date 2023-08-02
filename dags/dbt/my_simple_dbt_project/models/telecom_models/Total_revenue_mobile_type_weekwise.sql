{{ config( materialized='table', schema= 'Final') }}

select mobile_type, week_number,
sum(revenue_usd) Revenue  from
{{ref('Curated')}}
group by 1,2