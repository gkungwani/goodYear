{{ config( materialized='table', schema= 'Final') }}

select os_vendor,
sum(revenue_usd) Revenue from
{{ref('Curated')}}
group by 1