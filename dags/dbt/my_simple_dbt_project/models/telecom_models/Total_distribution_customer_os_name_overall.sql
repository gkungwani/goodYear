{{ config( materialized='table', schema= 'Final') }}

Select os_name, count(distinct msisdn)  Total_Customers
 from {{ref('Curated')}} group by 1