with latest as (
    {{ silver_latest('product_category_name_translation', ['product_category_name']) }}
)

select
    product_category_name,
    product_category_name_english,
    {{ silver_metadata() }},
    {{ dq_errors({
        'category_name_null': 'product_category_name is null',
    }) }} as _dq_errors
from latest
