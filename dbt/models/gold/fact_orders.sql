with item_metrics as (
    select
        order_id,
        count(*) as item_count,
        count(distinct product_id) as distinct_product_count,
        count(distinct seller_id) as distinct_seller_count,
        sum(price) as item_value,
        sum(freight_value) as freight_value
    from {{ ref('stg_order_items') }}
    group by order_id
),

payment_metrics as (
    select
        order_id,
        count(*) as payment_count,
        sum(payment_value) as payment_value,
        max(payment_installments) as max_payment_installments
    from {{ ref('stg_order_payments') }}
    group by order_id
),

review_metrics as (
    select
        order_id,
        count(*) as review_count,
        avg(review_score) as average_review_score
    from {{ ref('stg_order_reviews') }}
    group by order_id
)

select
    orders.order_id,
    orders.customer_id as customer_key,
    to_number(to_char(to_date(orders.purchased_at), 'YYYYMMDD')) as order_date_key,
    orders.order_status,
    orders.purchased_at,
    orders.approved_at,
    orders.delivered_to_carrier_at,
    orders.delivered_to_customer_at,
    orders.estimated_delivery_at,
    coalesce(items.item_count, 0) as item_count,
    coalesce(items.distinct_product_count, 0) as distinct_product_count,
    coalesce(items.distinct_seller_count, 0) as distinct_seller_count,
    coalesce(items.item_value, 0)::decimal(18, 2) as item_value,
    coalesce(items.freight_value, 0)::decimal(18, 2) as freight_value,
    coalesce(payments.payment_count, 0) as payment_count,
    coalesce(payments.payment_value, 0)::decimal(18, 2) as payment_value,
    payments.max_payment_installments,
    coalesce(reviews.review_count, 0) as review_count,
    reviews.average_review_score,
    datediff(day, orders.purchased_at, orders.delivered_to_customer_at) as delivery_days,
    case
        when orders.delivered_to_customer_at is null then null
        when orders.delivered_to_customer_at > orders.estimated_delivery_at then true
        else false
    end as is_late_delivery
from {{ ref('stg_orders') }} as orders
left join item_metrics as items using (order_id)
left join payment_metrics as payments using (order_id)
left join review_metrics as reviews using (order_id)

