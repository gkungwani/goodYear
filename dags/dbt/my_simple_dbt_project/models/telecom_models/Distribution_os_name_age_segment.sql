{{ config( materialized='table', schema= 'Final') }}

Select os_name, count(distinct msisdn) Total_Customers
from {{ref('Curated')}} where age between 20 and 30 group by 1
