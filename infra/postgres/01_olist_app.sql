-- Operational (OLTP) schema that simulates the e-commerce application database.
-- Every table carries created_at / updated_at / deleted_at so the batch path can
-- read changes by watermark and soft deletes are visible to it.

create schema if not exists olist_app;
create schema if not exists ops;

create or replace function ops.set_updated_at() returns trigger
language plpgsql as $$
begin
    new.updated_at := now();
    return new;
end
$$;

create table olist_app.customers (
    customer_id              text primary key,
    customer_unique_id       text,
    customer_zip_code_prefix text,
    customer_city            text,
    customer_state           text,
    created_at               timestamptz not null default now(),
    updated_at               timestamptz not null default now(),
    deleted_at               timestamptz
);

create table olist_app.sellers (
    seller_id              text primary key,
    seller_zip_code_prefix text,
    seller_city            text,
    seller_state           text,
    created_at             timestamptz not null default now(),
    updated_at             timestamptz not null default now(),
    deleted_at             timestamptz
);

-- Column names keep the original dataset's spelling ("lenght").
create table olist_app.products (
    product_id                 text primary key,
    product_category_name      text,
    product_name_lenght        numeric,
    product_description_lenght numeric,
    product_photos_qty         numeric,
    product_weight_g           numeric,
    product_length_cm          numeric,
    product_height_cm          numeric,
    product_width_cm           numeric,
    created_at                 timestamptz not null default now(),
    updated_at                 timestamptz not null default now(),
    deleted_at                 timestamptz
);

create table olist_app.product_category_name_translation (
    product_category_name         text primary key,
    product_category_name_english text,
    created_at                    timestamptz not null default now(),
    updated_at                    timestamptz not null default now(),
    deleted_at                    timestamptz
);

create table olist_app.orders (
    order_id                      text primary key,
    customer_id                   text,
    order_status                  text,
    order_purchase_timestamp      timestamp,
    order_approved_at             timestamp,
    order_delivered_carrier_date  timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp,
    created_at                    timestamptz not null default now(),
    updated_at                    timestamptz not null default now(),
    deleted_at                    timestamptz
);

create table olist_app.order_items (
    order_id            text,
    order_item_id       integer,
    product_id          text,
    seller_id           text,
    shipping_limit_date timestamp,
    price               numeric(12, 2),
    freight_value       numeric(12, 2),
    created_at          timestamptz not null default now(),
    updated_at          timestamptz not null default now(),
    deleted_at          timestamptz,
    primary key (order_id, order_item_id)
);

create table olist_app.order_payments (
    order_id             text,
    payment_sequential   integer,
    payment_type         text,
    payment_installments integer,
    payment_value        numeric(12, 2),
    created_at           timestamptz not null default now(),
    updated_at           timestamptz not null default now(),
    deleted_at           timestamptz,
    primary key (order_id, payment_sequential)
);

create table olist_app.order_reviews (
    review_id               text,
    order_id                text,
    review_score            integer,
    review_comment_title    text,
    review_comment_message  text,
    review_creation_date    timestamp,
    review_answer_timestamp timestamp,
    created_at              timestamptz not null default now(),
    updated_at              timestamptz not null default now(),
    deleted_at              timestamptz,
    primary key (review_id, order_id)
);

-- No foreign keys on purpose: orphan rows are something the pipeline's data
-- quality checks must catch, the way they would with a loosely coupled source.

-- One updated_at trigger and one updated_at index per table.
do $$
declare
    t text;
begin
    for t in
        select table_name from information_schema.tables where table_schema = 'olist_app'
    loop
        execute format(
            'create trigger %I before update on olist_app.%I '
            'for each row execute function ops.set_updated_at()',
            'trg_' || t || '_updated_at', t
        );
        execute format(
            'create index %I on olist_app.%I (updated_at)',
            'idx_' || t || '_updated_at', t
        );
    end loop;
end
$$;

-- Batch ingestion state: the last updated_at already copied to Bronze, per table.
create table ops.ingest_watermark (
    table_name     text primary key,
    last_watermark timestamptz not null,
    rows_last_run  bigint not null default 0,
    updated_at     timestamptz not null default now()
);
