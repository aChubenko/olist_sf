select
    seller_id as seller_key,
    seller_id,
    zip_code_prefix as geography_key
from {{ ref('stg_sellers') }}

