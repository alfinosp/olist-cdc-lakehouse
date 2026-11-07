{{ config(severity='warn', warn_if='>275') }}

-- Paid amount should be close to items + freight. The real dataset has some gaps
-- (vouchers, installment interest), so this only warns above a threshold.
-- Calibrated on the full dataset: 249 orders (0.25%) differ by more than 1 BRL
-- (vouchers, installment interest). 275 leaves ~10% headroom, so the warning
-- fires only when the gap grows.
select
    order_id,
    items_value + freight_value as expected_value,
    payment_value,
    payment_value - (items_value + freight_value) as difference
from {{ ref('fct_orders') }}
where items_count > 0
  and payment_value > 0
  and abs(payment_value - (items_value + freight_value)) > 1.00
