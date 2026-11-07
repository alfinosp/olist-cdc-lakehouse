with latest as (
    {{ silver_latest('customers', ['customer_id']) }}
)

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    upper(customer_state) as customer_state,
    {{ silver_metadata() }},
    {{ dq_errors({
        'customer_id_null': 'customer_id is null',
        'customer_state_invalid': 'customer_state is null or length(customer_state) != 2',
    }) }} as _dq_errors
from latest
