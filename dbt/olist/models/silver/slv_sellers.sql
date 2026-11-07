with latest as (
    {{ silver_latest('sellers', ['seller_id']) }}
)

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    upper(seller_state) as seller_state,
    {{ silver_metadata() }},
    {{ dq_errors({
        'seller_id_null': 'seller_id is null',
        'seller_state_invalid': 'seller_state is null or length(seller_state) != 2',
    }) }} as _dq_errors
from latest
