{#
    Latest version per primary key from a Bronze table.

    - Full refresh: rank every Bronze row.
    - Incremental: only rows ingested since the last run (minus a 10-minute
      lookback for late commits), and never a version older than the one
      Silver already holds. Re-running is therefore idempotent.
#}
{% macro silver_latest(bronze_table, pk_columns) %}
with src as (
    select
        *,
        concat_ws('|', {% for c in pk_columns %}coalesce({{ c }}, '<null>'){% if not loop.last %}, {% endif %}{% endfor %}) as _pk
    from {{ source('bronze', bronze_table) }}
    {% if is_incremental() %}
    where _ingested_at > (
        select coalesce(max(_ingested_at), timestamptz '1970-01-01') - interval 10 minute
        from {{ this }}
    )
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by _pk
            order by _source_ts desc, _lsn desc nulls last, _ingested_at desc
        ) as _rn
    from src
)

select * exclude (_rn)
from ranked
where _rn = 1
{% if is_incremental() %}
  and not exists (
      select 1 from {{ this }} as cur
      where cur._pk = ranked._pk and cur._source_ts > ranked._source_ts
  )
{% endif %}
{% endmacro %}


{# Comma-separated list of the rule names a row breaks; null when it breaks none. #}
{% macro dq_errors(rules) %}
nullif(concat_ws(',',
    {%- for name, condition in rules.items() %}
    case when {{ condition }} then '{{ name }}' end{% if not loop.last %},{% endif %}
    {%- endfor %}
), '')
{% endmacro %}


{# Metadata columns every Silver model carries. #}
{% macro silver_metadata() %}
    _pk,
    (_op = 'd' or deleted_at is not null) as _is_deleted,
    _op,
    _source,
    _source_ts,
    _lsn,
    _ingested_at
{% endmacro %}
