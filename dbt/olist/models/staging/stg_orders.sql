select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    cast(_source_ts as timestamp) as source_updated_at  -- UTC (profile sets TimeZone=UTC)
from {{ ref('slv_orders') }}
where _dq_errors is null and not _is_deleted
