select
    p.product_id,
    p.product_category_name,
    coalesce(t.product_category_name_english, p.product_category_name, 'unknown') as product_category,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    cast(p._source_ts as timestamp) as source_updated_at  -- UTC (profile sets TimeZone=UTC)
from {{ ref('slv_products') }} as p
left join {{ ref('slv_product_category_name_translation') }} as t
    on t.product_category_name = p.product_category_name
    and t._dq_errors is null
    and not t._is_deleted
where p._dq_errors is null and not p._is_deleted
