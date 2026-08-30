with ranked as (
    select
        nullif(trim(order_id), '') as order_id,
        try_to_number(payment_sequential) as payment_sequential,
        lower(nullif(trim(payment_type), '')) as payment_type,
        try_to_number(payment_installments) as payment_installments,
        try_to_decimal(payment_value, 18, 2) as payment_value,
        _loaded_at,
        row_number() over (
            partition by order_id, payment_sequential
            order by _loaded_at desc, _source_row_number desc
        ) as row_rank
    from {{ source('bronze', 'raw_order_payments') }}
)

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    _loaded_at
from ranked
where row_rank = 1

