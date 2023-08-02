{{ config( materialized='table', schema= 'Final') }}

select age,
sum(revenue_usd) Revenue  from
{{ref('Curated')}}
group by 1