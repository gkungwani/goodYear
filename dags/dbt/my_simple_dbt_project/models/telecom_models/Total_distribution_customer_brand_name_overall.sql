{{ config( materialized='table', schema= 'Final') }}

Select brand_name, count(distinct msisdn)  Total_Customers
 from {{ref('Curated')}} group by 1

