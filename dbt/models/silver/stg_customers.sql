with ranked as (
    select
        nullif(trim(customer_id), '') as customer_id,
        nullif(trim(customer_unique_id), '') as customer_unique_id,
        lpad(nullif(trim(customer_zip_code_prefix), ''), 5, '0') as zip_code_prefix,
        nullif(trim(customer_city), '') as city,
        upper(nullif(trim(customer_state), '')) as state_code,
        _loaded_at,
        row_number() over (
            partition by customer_id
            order by _loaded_at desc, _source_row_number desc
        ) as row_rank
    from {{ source('bronze', 'raw_customers') }}
)

select
    customer_id,
    customer_unique_id,
    zip_code_prefix,
    city,
    state_code,
    _loaded_at
from ranked
where row_rank = 1

