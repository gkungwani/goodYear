{{ config( materialized='table', schema= 'Final') }}

Select week_number, brand_name, count(distinct msisdn)  Total_Customers
 from {{ref('Curated')}} group by 1,2