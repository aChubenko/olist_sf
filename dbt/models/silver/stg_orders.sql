with ranked as (
    select
        nullif(trim(order_id), '') as order_id,
        nullif(trim(customer_id), '') as customer_id,
        lower(nullif(trim(order_status), '')) as order_status,
        try_to_timestamp_ntz(order_purchase_timestamp) as purchased_at,
        try_to_timestamp_ntz(order_approved_at) as approved_at,
        try_to_timestamp_ntz(order_delivered_carrier_date) as delivered_to_carrier_at,
        try_to_timestamp_ntz(order_delivered_customer_date) as delivered_to_customer_at,
        try_to_timestamp_ntz(order_estimated_delivery_date) as estimated_delivery_at,
        _loaded_at,
        row_number() over (
            partition by order_id
            order by _loaded_at desc, _source_row_number desc
        ) as row_rank
    from {{ source('bronze', 'raw_orders') }}
)

select
    order_id,
    customer_id,
    order_status,
    purchased_at,
    approved_at,
    delivered_to_carrier_at,
    delivered_to_customer_at,
    estimated_delivery_at,
    _loaded_at
from ranked
where row_rank = 1

