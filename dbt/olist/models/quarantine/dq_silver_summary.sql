{# Active and quarantined row counts per Silver model, the input to the quarantine ratio test. #}
{% set silver_models = [
    'slv_customers', 'slv_sellers', 'slv_products', 'slv_product_category_name_translation',
    'slv_orders', 'slv_order_items', 'slv_order_payments', 'slv_order_reviews',
] %}

{% for m in silver_models %}
select
    '{{ m }}' as model_name,
    count(*) as active_rows,
    count(*) filter (where _dq_errors is not null) as quarantined_rows
from {{ ref(m) }}
where not _is_deleted
{% if not loop.last %}union all{% endif %}
{% endfor %}
