with bounds as (
    select
        coalesce(min(to_date(purchased_at)), '2016-01-01'::date) as min_date,
        coalesce(
            max(
                greatest_ignore_nulls(
                    to_date(purchased_at),
                    to_date(delivered_to_customer_at),
                    to_date(estimated_delivery_at)
                )
            ),
            '2018-12-31'::date
        ) as max_date
    from {{ ref('stg_orders') }}
),

numbers as (
    select row_number() over (order by seq4()) - 1 as day_number
    from table(generator(rowcount => 5000))
),

dates as (
    select dateadd(day, numbers.day_number, bounds.min_date)::date as date_day
    from numbers
    cross join bounds
    where dateadd(day, numbers.day_number, bounds.min_date)::date <= bounds.max_date
)

select
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
    date_day,
    year(date_day) as year_number,
    quarter(date_day) as quarter_number,
    month(date_day) as month_number,
    monthname(date_day) as month_name,
    weekofyear(date_day) as week_of_year,
    dayofmonth(date_day) as day_of_month,
    dayofweekiso(date_day) as day_of_week,
    dayname(date_day) as day_name,
    dayofweekiso(date_day) in (6, 7) as is_weekend
from dates

