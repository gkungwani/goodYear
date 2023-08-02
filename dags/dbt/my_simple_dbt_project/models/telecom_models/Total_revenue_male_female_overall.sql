{{ config( materialized='table', schema= 'Final') }}

select gender,
sum(revenue_usd) Revenue  from
{{ref('Curated')}}
group by 1