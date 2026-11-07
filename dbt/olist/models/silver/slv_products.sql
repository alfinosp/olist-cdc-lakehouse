with latest as (
    {{ silver_latest('products', ['product_id']) }}
),

typed as (
    select
        *,
        try_cast(product_name_lenght as double)        as name_length,
        try_cast(product_description_lenght as double) as description_length,
        try_cast(product_photos_qty as double)         as photos_qty,
        try_cast(product_weight_g as double)           as weight_g,
        try_cast(product_length_cm as double)          as length_cm,
        try_cast(product_height_cm as double)          as height_cm,
        try_cast(product_width_cm as double)           as width_cm
    from latest
)

select
    product_id,
    product_category_name,
    cast(name_length as integer)        as product_name_length,
    cast(description_length as integer) as product_description_length,
    cast(photos_qty as integer)         as product_photos_qty,
    weight_g                            as product_weight_g,
    length_cm                           as product_length_cm,
    height_cm                           as product_height_cm,
    width_cm                            as product_width_cm,
    {{ silver_metadata() }},
    {{ dq_errors({
        'product_id_null': 'product_id is null',
        'weight_unparseable': 'product_weight_g is not null and weight_g is null',
        'weight_negative': 'weight_g < 0',
    }) }} as _dq_errors
from typed
