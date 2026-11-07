{% macro create_dq_audit_log() %}
    create schema if not exists audit;
    create table if not exists audit.dq_audit_log (
        run_id           varchar,
        run_at           timestamptz,
        node_name        varchar,
        status           varchar,
        failures         bigint,
        severity         varchar,
        execution_time_s double
    )
{% endmacro %}


{# Called from on-run-end: one audit row per data test executed in this run. #}
{% macro log_dq_results(results) %}
    {%- if not execute -%}{{ return('select 1') }}{%- endif -%}
    {%- set tests = [] -%}
    {%- for r in results -%}
        {%- if r.node.resource_type == 'test' -%}{%- do tests.append(r) -%}{%- endif -%}
    {%- endfor -%}
    {%- if tests | length == 0 -%}{{ return('select 1') }}{%- endif -%}
    insert into audit.dq_audit_log values
    {%- for r in tests %}
    (
        '{{ invocation_id }}',
        timestamptz '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}+00',
        '{{ r.node.name }}',
        '{{ r.status }}',
        {{ r.failures if r.failures is not none else 'null' }},
        '{{ r.node.config.severity | lower }}',
        {{ r.execution_time | round(3) }}
    ){% if not loop.last %},{% endif %}
    {%- endfor %}
{% endmacro %}
