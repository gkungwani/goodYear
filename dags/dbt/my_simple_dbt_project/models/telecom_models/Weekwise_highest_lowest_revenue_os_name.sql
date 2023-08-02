{{ config( materialized='table', schema= 'Final') }}

select
    a.week_number,
    a.os_name as brand_with_min_rev,
    a.rev as min_rev,
    b.os_name as brand_with_max_rev,
    b.rev as max_rev

from
    (select
        week_number,
        os_name,
        sum(revenue_usd) as rev,
        row_number() over(partition by week_number order by sum(revenue_usd) asc) as rw_asc
    from
        {{ref('Curated')}}
    group by
        1,2
    ) as a

join

    (select
        week_number,
        os_name,
        sum(revenue_usd) as rev,
        row_number() over(partition by week_number order by sum(revenue_usd) desc) as rw_desc
    from
        {{ref('Curated')}}
    group by
        1,2
    ) as b
on
    a.week_number = b.week_number
    and a.rw_asc = b.rw_desc
where
    a.rw_asc = 1
    and b.rw_desc = 1
order by
    1