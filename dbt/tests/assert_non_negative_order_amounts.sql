select order_id
from {{ ref('fact_orders') }}
where item_value < 0
   or freight_value < 0
   or payment_value < 0

