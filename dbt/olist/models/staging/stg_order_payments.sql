select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    cast(_source_ts as timestamp) as source_updated_at  -- UTC (profile sets TimeZone=UTC)
from {{ ref('slv_order_payments') }}
where _dq_errors is null and not _is_deleted
