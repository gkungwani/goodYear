{{ config( materialized='table', schema= 'Final') }}

select brand_name,
sum(revenue_usd) Revenue from
{{ref('Curated')}}
group by 1