-- Grain: one row per order (accumulating snapshot: one timestamp per lifecycle step).
with items as (
    select
        order_id,
        count(*)           as items_count,
        sum(price)         as items_value,
        sum(freight_value) as freight_value
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(payment_value) as payment_value
    from {{ ref('stg_order_payments') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    cast(strftime(o.order_purchase_timestamp, '%Y%m%d') as integer) as purchase_date_key,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    coalesce(i.items_count, 0)     as items_count,
    coalesce(i.items_value, 0)     as items_value,
    coalesce(i.freight_value, 0)   as freight_value,
    coalesce(p.payment_value, 0)   as payment_value,
    date_diff('day', o.order_purchase_timestamp, o.order_delivered_customer_date) as delivery_days,
    o.order_delivered_customer_date > o.order_estimated_delivery_date as is_late
from {{ ref('stg_orders') }} as o
left join items as i on i.order_id = o.order_id
left join payments as p on p.order_id = o.order_id
