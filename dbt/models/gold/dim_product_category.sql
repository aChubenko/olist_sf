select
    category_name as product_category_key,
    category_name,
    category_name_english
from {{ ref('stg_product_categories') }}

