-- Grain: one row per item in an order.
select
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    o.customer_id,
    cast(strftime(o.order_purchase_timestamp, '%Y%m%d') as integer) as purchase_date_key,
    i.shipping_limit_date,
    i.price,
    i.freight_value,
    i.price + i.freight_value as item_total
from {{ ref('stg_order_items') }} as i
inner join {{ ref('stg_orders') }} as o
    on o.order_id = i.order_id
