{{
  config(
    materialized='table'
  )
}}
select * from {{ref('Curated')}}
{{ unload_to_s3('Test') }}