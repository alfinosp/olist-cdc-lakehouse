-- Grain: one row per payment of an order.
select
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value,
    cast(strftime(o.order_purchase_timestamp, '%Y%m%d') as integer) as purchase_date_key
from {{ ref('stg_order_payments') }} as p
inner join {{ ref('stg_orders') }} as o
    on o.order_id = p.order_id
