with latest as (
    {{ silver_latest('orders', ['order_id']) }}
),

typed as (
    select
        *,
        try_cast(order_purchase_timestamp as timestamp)      as purchase_ts,
        try_cast(order_approved_at as timestamp)             as approved_ts,
        try_cast(order_delivered_carrier_date as timestamp)  as carrier_ts,
        try_cast(order_delivered_customer_date as timestamp) as delivered_ts,
        try_cast(order_estimated_delivery_date as timestamp) as estimated_ts
    from latest
)

select
    order_id,
    customer_id,
    order_status,
    purchase_ts  as order_purchase_timestamp,
    approved_ts  as order_approved_at,
    carrier_ts   as order_delivered_carrier_date,
    delivered_ts as order_delivered_customer_date,
    estimated_ts as order_estimated_delivery_date,
    {{ silver_metadata() }},
    {{ dq_errors({
        'order_id_null': 'order_id is null',
        'customer_id_null': 'customer_id is null',
        'order_status_invalid': "order_status is null or order_status not in ('created', 'approved', 'invoiced', 'processing', 'shipped', 'delivered', 'canceled', 'unavailable')",
        'purchase_ts_null': 'purchase_ts is null',
        'purchase_ts_unparseable': 'order_purchase_timestamp is not null and purchase_ts is null',
        'delivered_ts_unparseable': 'order_delivered_customer_date is not null and delivered_ts is null',
    }) }} as _dq_errors
from typed
