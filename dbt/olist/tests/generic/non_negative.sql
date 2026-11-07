{# Fails on rows where the column is below zero. Nulls pass (use not_null for those). #}
{% test non_negative(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
