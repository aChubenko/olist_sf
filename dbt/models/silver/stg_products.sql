with ranked as (
    select
        nullif(trim(product_id), '') as product_id,
        nullif(trim(product_category_name), '') as category_name,
        try_to_number(product_name_lenght) as name_length,
        try_to_number(product_description_lenght) as description_length,
        try_to_number(product_photos_qty) as photos_quantity,
        try_to_decimal(product_weight_g, 18, 2) as weight_g,
        try_to_decimal(product_length_cm, 18, 2) as length_cm,
        try_to_decimal(product_height_cm, 18, 2) as height_cm,
        try_to_decimal(product_width_cm, 18, 2) as width_cm,
        _loaded_at,
        row_number() over (
            partition by product_id
            order by _loaded_at desc, _source_row_number desc
        ) as row_rank
    from {{ source('bronze', 'raw_products') }}
)

select
    product_id,
    category_name,
    name_length,
    description_length,
    photos_quantity,
    weight_g,
    length_cm,
    height_cm,
    width_cm,
    _loaded_at
from ranked
where row_rank = 1

