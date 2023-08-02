{{ config( materialized='table', schema= 'Final') }}

select value_segment,
sum(revenue_usd) Revenue  from
{{ref('Curated')}}
group by 1