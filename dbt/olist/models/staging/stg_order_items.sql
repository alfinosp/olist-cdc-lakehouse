select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    cast(_source_ts as timestamp) as source_updated_at  -- UTC (profile sets TimeZone=UTC)
from {{ ref('slv_order_items') }}
where _dq_errors is null and not _is_deleted
