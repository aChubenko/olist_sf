select
    items.order_id,
    items.order_item_id,
    orders.customer_id as customer_key,
    items.product_id as product_key,
    items.seller_id as seller_key,
    to_number(to_char(to_date(orders.purchased_at), 'YYYYMMDD')) as order_date_key,
    items.shipping_limit_at,
    items.price,
    items.freight_value,
    items.price + items.freight_value as gross_item_value
from {{ ref('stg_order_items') }} as items
inner join {{ ref('stg_orders') }} as orders using (order_id)

