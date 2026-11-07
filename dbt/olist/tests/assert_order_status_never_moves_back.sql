{{ config(severity='warn') }}

-- Across the SCD2 history, an order's status may only move forward
-- (canceled / unavailable are terminal and not ranked).
with history as (
    select
        order_id,
        order_status,
        dbt_valid_from,
        case order_status
            when 'created' then 1
            when 'approved' then 2
            when 'invoiced' then 3
            when 'processing' then 4
            when 'shipped' then 5
            when 'delivered' then 6
        end as status_rank
    from {{ ref('snap_orders') }}
),

sequenced as (
    select
        *,
        lead(status_rank) over (partition by order_id order by dbt_valid_from) as next_rank
    from history
)

select *
from sequenced
where status_rank is not null
  and next_rank is not null
  and next_rank < status_rank
