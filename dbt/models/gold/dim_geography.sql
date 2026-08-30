with candidates as (
    select
        zip_code_prefix,
        city,
        state_code,
        latitude,
        longitude,
        1 as source_priority
    from {{ ref('stg_geolocation') }}

    union all

    select
        zip_code_prefix,
        min(city) as city,
        min(state_code) as state_code,
        null::float as latitude,
        null::float as longitude,
        2 as source_priority
    from {{ ref('stg_customers') }}
    where zip_code_prefix is not null
    group by zip_code_prefix

    union all

    select
        zip_code_prefix,
        min(city) as city,
        min(state_code) as state_code,
        null::float as latitude,
        null::float as longitude,
        3 as source_priority
    from {{ ref('stg_sellers') }}
    where zip_code_prefix is not null
    group by zip_code_prefix
),

ranked as (
    select
        zip_code_prefix as geography_key,
        zip_code_prefix,
        city,
        state_code,
        latitude,
        longitude,
        row_number() over (
            partition by zip_code_prefix
            order by source_priority
        ) as row_rank
    from candidates
)

select
    geography_key,
    zip_code_prefix,
    city,
    state_code,
    latitude,
    longitude
from ranked
where row_rank = 1

