with typed as (
    select
        lpad(nullif(trim(geolocation_zip_code_prefix), ''), 5, '0') as zip_code_prefix,
        try_to_double(geolocation_lat) as latitude,
        try_to_double(geolocation_lng) as longitude,
        nullif(trim(geolocation_city), '') as city,
        upper(nullif(trim(geolocation_state), '')) as state_code,
        _loaded_at
    from {{ source('bronze', 'raw_geolocation') }}
    where nullif(trim(geolocation_zip_code_prefix), '') is not null
)

select
    zip_code_prefix,
    min(city) as city,
    min(state_code) as state_code,
    avg(latitude) as latitude,
    avg(longitude) as longitude,
    max(_loaded_at) as _loaded_at
from typed
group by zip_code_prefix

