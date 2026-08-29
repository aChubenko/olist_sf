with ranked as (
    select
        nullif(trim(order_id), '') as order_id,
        try_to_number(order_item_id) as order_item_id,
        nullif(trim(product_id), '') as product_id,
        nullif(trim(seller_id), '') as seller_id,
        try_to_timestamp_ntz(shipping_limit_date) as shipping_limit_at,
        try_to_decimal(price, 18, 2) as price,
        try_to_decimal(freight_value, 18, 2) as freight_value,
        _loaded_at,
        row_number() over (
            partition by order_id, order_item_id
            order by _loaded_at desc, _source_row_number desc
        ) as row_rank
    from {{ source('bronze', 'raw_order_items') }}
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_at,
    price,
    freight_value,
    _loaded_at
from ranked
where row_rank = 1

