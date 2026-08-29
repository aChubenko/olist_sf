select
    dates.date_key,
    dates.date_day,
    dates.year_number,
    dates.month_number,
    count(orders.order_id) as order_count,
    count(distinct orders.customer_key) as customer_count,
    count_if(orders.order_status = 'delivered') as delivered_order_count,
    sum(orders.item_count) as item_count,
    sum(orders.item_value) as item_value,
    sum(orders.freight_value) as freight_value,
    sum(orders.payment_value) as payment_value,
    avg(orders.average_review_score) as average_review_score,
    avg(orders.delivery_days) as average_delivery_days,
    count_if(orders.is_late_delivery) as late_delivery_count
from {{ ref('dim_date') }} as dates
inner join {{ ref('fact_orders') }} as orders
    on dates.date_key = orders.order_date_key
group by
    dates.date_key,
    dates.date_day,
    dates.year_number,
    dates.month_number

