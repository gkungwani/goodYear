{{ config( materialized='table', schema= 'Final') }}

select os_name, week_number,
sum(revenue_usd) Revenue from
{{ref('Curated')}}
group by 1,2