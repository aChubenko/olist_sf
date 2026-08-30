select
    product_id as product_key,
    product_id,
    category_name as product_category_key,
    name_length,
    description_length,
    photos_quantity,
    weight_g,
    length_cm,
    height_cm,
    width_cm,
    length_cm * height_cm * width_cm as volume_cm3
from {{ ref('stg_products') }}

