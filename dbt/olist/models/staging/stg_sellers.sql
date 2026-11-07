select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    cast(_source_ts as timestamp) as source_updated_at  -- UTC (profile sets TimeZone=UTC)
from {{ ref('slv_sellers') }}
where _dq_errors is null and not _is_deleted
