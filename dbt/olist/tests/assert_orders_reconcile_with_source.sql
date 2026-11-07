{{ config(severity='error', enabled=(target.name != 'ci')) }}

-- Active orders in Silver must match active orders in Postgres as of the point Silver
-- has caught up to. That point is the later of
--   - the batch watermark (rows changed after it are not ingested yet), and
--   - the newest CDC commit time in Silver (Debezium delivers a table's changes in
--     commit order, so every earlier change has arrived).
-- Needs the Postgres attach from the dev profile, so it is disabled in CI.
with cutoff as (
    select greatest(
        coalesce(
            (select max(last_watermark) from pg.ops.ingest_watermark where table_name = 'orders'),
            timestamptz '1970-01-01'
        ),
        coalesce(
            (select max(_source_ts) from {{ ref('slv_orders') }} where _source = 'cdc'),
            timestamptz '1970-01-01'
        )
    ) as ts
),

source_rows as (
    select count(*) as n
    from pg.olist_app.orders as o, cutoff
    where o.created_at <= cutoff.ts
      and (o.deleted_at is null or o.deleted_at > cutoff.ts)
),

silver_rows as (
    select count(*) as n
    from {{ ref('slv_orders') }}
    where not _is_deleted
)

select
    s.n as source_rows,
    t.n as silver_rows,
    s.n - t.n as difference
from source_rows as s, silver_rows as t
where abs(s.n - t.n) > {{ var('reconciliation_tolerance') }} * s.n
