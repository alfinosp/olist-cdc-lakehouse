with latest as (
    {{ silver_latest('order_reviews', ['review_id', 'order_id']) }}
),

typed as (
    select
        *,
        try_cast(review_score as integer)              as score,
        try_cast(review_creation_date as timestamp)    as created_ts,
        try_cast(review_answer_timestamp as timestamp) as answered_ts
    from latest
)

select
    review_id,
    order_id,
    score       as review_score,
    review_comment_title,
    review_comment_message,
    created_ts  as review_creation_date,
    answered_ts as review_answer_timestamp,
    {{ silver_metadata() }},
    {{ dq_errors({
        'review_id_null': 'review_id is null',
        'order_id_null': 'order_id is null',
        'review_score_out_of_range': 'score is null or score not between 1 and 5',
    }) }} as _dq_errors
from typed
