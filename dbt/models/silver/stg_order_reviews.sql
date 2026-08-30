with ranked as (
    select
        nullif(trim(review_id), '') as review_id,
        nullif(trim(order_id), '') as order_id,
        try_to_number(review_score) as review_score,
        nullif(trim(review_comment_title), '') as review_comment_title,
        nullif(trim(review_comment_message), '') as review_comment_message,
        try_to_timestamp_ntz(review_creation_date) as review_created_at,
        try_to_timestamp_ntz(review_answer_timestamp) as review_answered_at,
        _loaded_at,
        row_number() over (
            partition by review_id, order_id
            order by _loaded_at desc, _source_row_number desc
        ) as row_rank
    from {{ source('bronze', 'raw_order_reviews') }}
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_created_at,
    review_answered_at,
    _loaded_at
from ranked
where row_rank = 1

