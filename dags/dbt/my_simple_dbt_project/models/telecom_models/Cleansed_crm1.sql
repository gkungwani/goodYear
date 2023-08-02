{{ config(post_hook="update {{this}}
set year_of_birth=(select ROUND(sum(year_of_birth * cnt) / sum(cnt))
from (select year_of_birth,count(*) as cnt from {{this}} group by
year_of_birth) ) where year_of_birth is null"
, materialized='table') }}

select
   * exclude rw

from
    (select
        msisdn,
        case
            when gender in ('N/A',null) then 'Not Available'
            when editdistance(UPPER(gender),'FEMALE')<editdistance(UPPER(gender),'MALE') then 'MALE'
            else 'FEMALE'
        end as gender,
        year_of_birth,
        mobile_type,
        value_segment,
        system_status,
        row_number() over(partition by msisdn order by random()) as rw
    from telecom_database.raw.crm1
    ) as a
where
    rw = 1

