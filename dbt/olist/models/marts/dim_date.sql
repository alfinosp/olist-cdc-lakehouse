select
    cast(d as date)                         as date_day,
    cast(strftime(d, '%Y%m%d') as integer)  as date_key,
    year(d)                                 as year,
    quarter(d)                              as quarter,
    month(d)                                as month,
    monthname(d)                            as month_name,
    day(d)                                  as day_of_month,
    isodow(d)                               as day_of_week,
    dayname(d)                              as day_name,
    isodow(d) in (6, 7)                     as is_weekend
from range(date '2016-01-01', date '2031-01-01', interval 1 day) as t(d)
