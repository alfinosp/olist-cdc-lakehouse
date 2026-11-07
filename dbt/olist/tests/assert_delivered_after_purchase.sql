{{ config(severity='warn') }}

-- An order cannot reach the customer before it was bought.
select
    order_id,
    order_purchase_timestamp,
    order_delivered_customer_date
from {{ ref('fct_orders') }}
where order_delivered_customer_date < order_purchase_timestamp
