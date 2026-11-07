with latest as (
    {{ silver_latest('order_payments', ['order_id', 'payment_sequential']) }}
),

typed as (
    select
        *,
        try_cast(payment_sequential as integer)   as seq,
        try_cast(payment_installments as integer) as installments,
        try_cast(payment_value as decimal(12, 2)) as value_num
    from latest
)

select
    order_id,
    seq          as payment_sequential,
    payment_type,
    installments as payment_installments,
    value_num    as payment_value,
    {{ silver_metadata() }},
    {{ dq_errors({
        'order_id_null': 'order_id is null',
        'payment_sequential_invalid': 'seq is null',
        'payment_type_invalid': "payment_type is null or payment_type not in ('credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined')",
        'payment_value_unparseable': 'payment_value is not null and value_num is null',
        'payment_value_negative': 'value_num < 0',
    }) }} as _dq_errors
from typed
