with category_names as (
    select category_name
    from {{ ref('stg_product_categories') }}

    union

    select category_name
    from {{ ref('stg_products') }}
    where category_name is not null
),

translations as (
    select
        category_name,
        category_name_english
    from {{ ref('stg_product_categories') }}
)

select
    category_names.category_name as product_category_key,
    category_names.category_name,
    translations.category_name_english
from category_names
left join translations
    on category_names.category_name = translations.category_name
