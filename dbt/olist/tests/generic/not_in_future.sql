{# Fails on rows whose timestamp is later than now plus a tolerance (default 24 hours). #}
{% test not_in_future(model, column_name, tolerance_hours=24) %}
select *
from {{ model }}
where {{ column_name }} > cast(now() as timestamp) + interval ({{ tolerance_hours }}) hour
{% endtest %}
