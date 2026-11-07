{{ config(severity='error') }}

-- Stop the pipeline when a Silver table quarantines more than the allowed share
-- of its rows: at that point the source is broken, not a few bad records.
select
    model_name,
    active_rows,
    quarantined_rows,
    round(quarantined_rows / active_rows, 4) as quarantined_ratio
from {{ ref('dq_silver_summary') }}
where active_rows > 0
  and quarantined_rows > {{ var('quarantine_error_ratio') }} * active_rows
