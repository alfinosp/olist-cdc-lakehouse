{# Every active Silver row that broke at least one validation rule. These rows never reach Gold. #}
{% set silver_models = [
    'slv_customers', 'slv_sellers', 'slv_products', 'slv_product_category_name_translation',
    'slv_orders', 'slv_order_items', 'slv_order_payments', 'slv_order_reviews',
] %}

{% for m in silver_models %}
select
    '{{ m }}' as model_name,
    _pk,
    _dq_errors,
    _source_ts,
    _ingested_at
from {{ ref(m) }}
where _dq_errors is not null and not _is_deleted
{% if not loop.last %}union all{% endif %}
{% endfor %}
