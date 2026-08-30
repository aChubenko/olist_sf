select
    customer_id as customer_key,
    customer_id,
    customer_unique_id,
    zip_code_prefix as geography_key
from {{ ref('stg_customers') }}

