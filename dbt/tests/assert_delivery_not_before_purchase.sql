select order_id
from {{ ref('fact_orders') }}
where delivered_to_customer_at < purchased_at

