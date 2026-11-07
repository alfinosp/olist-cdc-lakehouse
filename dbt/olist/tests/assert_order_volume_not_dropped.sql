{{ config(severity='warn') }}

-- Warn when orders placed in the last 24 hours fall below half the daily average
-- of the 7 days before. While the simulator runs this is steady; a drop usually
-- means the source or the ingestion stopped.
with daily as (
    select
        count(*) filter (
            where order_purchase_timestamp >= cast(now() as timestamp) - interval 1 day
        ) as last_24h,
        count(*) filter (
            where order_purchase_timestamp >= cast(now() as timestamp) - interval 8 day
              and order_purchase_timestamp <  cast(now() as timestamp) - interval 1 day
        ) / 7.0 as prev_7d_daily_avg
    from {{ ref('fct_orders') }}
)

select *
from daily
where prev_7d_daily_avg > 0
  and last_24h < 0.5 * prev_7d_daily_avg
