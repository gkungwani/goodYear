{{ config( materialized='table', schema= 'Final') }}

Select brand_name, count(distinct msisdn) Total_Customers
from {{ref('Curated')}} where age between 20 and 30 group by 1

