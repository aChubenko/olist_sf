with ranked as (
    select
        nullif(trim(seller_id), '') as seller_id,
        lpad(nullif(trim(seller_zip_code_prefix), ''), 5, '0') as zip_code_prefix,
        nullif(trim(seller_city), '') as city,
        upper(nullif(trim(seller_state), '')) as state_code,
        _loaded_at,
        row_number() over (
            partition by seller_id
            order by _loaded_at desc, _source_row_number desc
        ) as row_rank
    from {{ source('bronze', 'raw_sellers') }}
)

select
    seller_id,
    zip_code_prefix,
    city,
    state_code,
    _loaded_at
from ranked
where row_rank = 1

