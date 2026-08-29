with ranked as (
    select
        nullif(trim(product_category_name), '') as category_name,
        nullif(trim(product_category_name_english), '') as category_name_english,
        _loaded_at,
        row_number() over (
            partition by product_category_name
            order by _loaded_at desc, _source_row_number desc
        ) as row_rank
    from {{ source('bronze', 'raw_product_category_translation') }}
)

select
    category_name,
    category_name_english,
    _loaded_at
from ranked
where row_rank = 1

