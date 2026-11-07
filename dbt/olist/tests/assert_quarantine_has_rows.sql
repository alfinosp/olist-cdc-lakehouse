{{ config(severity='warn') }}

-- Warn when any row is quarantined, so the run summary in Slack lists it.
select *
from {{ ref('quarantine_rows') }}
