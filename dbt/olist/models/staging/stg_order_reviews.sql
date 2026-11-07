select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    cast(_source_ts as timestamp) as source_updated_at  -- UTC (profile sets TimeZone=UTC)
from {{ ref('slv_order_reviews') }}
where _dq_errors is null and not _is_deleted
