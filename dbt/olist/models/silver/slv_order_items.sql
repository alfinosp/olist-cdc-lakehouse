with latest as (
    {{ silver_latest('order_items', ['order_id', 'order_item_id']) }}
),

typed as (
    select
        *,
        try_cast(order_item_id as integer)         as item_no,
        try_cast(shipping_limit_date as timestamp) as shipping_limit_ts,
        try_cast(price as decimal(12, 2))          as price_num,
        try_cast(freight_value as decimal(12, 2))  as freight_num
    from latest
)

select
    order_id,
    item_no           as order_item_id,
    product_id,
    seller_id,
    shipping_limit_ts as shipping_limit_date,
    price_num         as price,
    freight_num       as freight_value,
    {{ silver_metadata() }},
    {{ dq_errors({
        'order_id_null': 'order_id is null',
        'order_item_id_invalid': 'item_no is null',
        'product_id_null': 'product_id is null',
        'seller_id_null': 'seller_id is null',
        'price_unparseable': 'price is not null and price_num is null',
        'price_negative': 'price_num < 0',
        'freight_negative': 'freight_num < 0',
    }) }} as _dq_errors
from typed
