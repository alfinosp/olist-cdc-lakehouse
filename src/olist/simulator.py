"""Simulate application traffic on the olist_app database.

Each event runs in its own transaction:
    new_order      a customer places an order (1-3 items + one payment)
    advance_order  an open order moves to its next status
    add_review     a delivered order without a review gets one
    delete_review  a review is soft-deleted (deleted_at is set)

With --bad-rate > 0 some events write rows that break data quality rules
(unknown status, negative price, orphan product, review score out of range),
so the Silver quarantine and the dbt tests have something to catch.

Usage:
    python -m olist.simulator --events-per-minute 60
    python -m olist.simulator --max-events 200 --bad-rate 0.2 --seed 42
"""

import argparse
import logging
import random
import sys
import time
import uuid

import psycopg
from psycopg import sql

from olist.config import pg_conninfo

log = logging.getLogger("simulator")

STATUS_FLOW = ["created", "approved", "invoiced", "processing", "shipped", "delivered"]
TERMINAL = {"delivered", "canceled", "unavailable"}
STATUS_TIMESTAMP = {
    "approved": "order_approved_at",
    "shipped": "order_delivered_carrier_date",
    "delivered": "order_delivered_customer_date",
}
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
EVENT_WEIGHTS = {"new_order": 0.35, "advance_order": 0.45, "add_review": 0.15, "delete_review": 0.05}
BAD_KINDS = ["unknown_status", "negative_price", "orphan_product"]


def next_status(current: str, rng: random.Random, cancel_rate: float = 0.03) -> str | None:
    """Next status in the order lifecycle; None when the order cannot move."""
    if current in TERMINAL or current not in STATUS_FLOW:
        return None
    if current != "shipped" and rng.random() < cancel_rate:
        return "canceled"
    return STATUS_FLOW[STATUS_FLOW.index(current) + 1]


def pick_event(rng: random.Random) -> str:
    return rng.choices(list(EVENT_WEIGHTS), weights=list(EVENT_WEIGHTS.values()))[0]


def new_id() -> str:
    return uuid.uuid4().hex


def random_row(cur: psycopg.Cursor, query: str) -> tuple | None:
    cur.execute(query)
    return cur.fetchone()


def new_order(cur: psycopg.Cursor, rng: random.Random, bad_kind: str | None) -> str:
    template = random_row(
        cur,
        "select customer_unique_id, customer_zip_code_prefix, customer_city, customer_state "
        "from olist_app.customers order by random() limit 1",
    )
    customer_id, order_id = new_id(), new_id()
    cur.execute(
        "insert into olist_app.customers (customer_id, customer_unique_id, "
        "customer_zip_code_prefix, customer_city, customer_state) values (%s, %s, %s, %s, %s)",
        (customer_id, *template),
    )
    status = "unknown_status" if bad_kind == "unknown_status" else "created"
    cur.execute(
        "insert into olist_app.orders (order_id, customer_id, order_status, "
        "order_purchase_timestamp, order_estimated_delivery_date) "
        "values (%s, %s, %s, now() at time zone 'UTC', "
        "(now() at time zone 'UTC') + make_interval(days => %s))",
        (order_id, customer_id, status, rng.randint(7, 30)),
    )

    total = 0.0
    for item_no in range(1, rng.randint(1, 3) + 1):
        product_id = random_row(
            cur, "select product_id from olist_app.products order by random() limit 1"
        )[0]
        if bad_kind == "orphan_product" and item_no == 1:
            product_id = "missing_" + new_id()[:24]
        seller_id = random_row(
            cur, "select seller_id from olist_app.sellers order by random() limit 1"
        )[0]
        price = round(rng.uniform(10, 500), 2)
        if bad_kind == "negative_price" and item_no == 1:
            price = -price
        freight = round(rng.uniform(5, 50), 2)
        total += price + freight
        cur.execute(
            "insert into olist_app.order_items (order_id, order_item_id, product_id, seller_id, "
            "shipping_limit_date, price, freight_value) "
            "values (%s, %s, %s, %s, (now() at time zone 'UTC') + interval '3 days', %s, %s)",
            (order_id, item_no, product_id, seller_id, price, freight),
        )

    cur.execute(
        "insert into olist_app.order_payments (order_id, payment_sequential, payment_type, "
        "payment_installments, payment_value) values (%s, 1, %s, %s, %s)",
        (order_id, rng.choice(PAYMENT_TYPES), rng.randint(1, 10), round(total, 2)),
    )
    return f"new_order {order_id}" + (f" [bad: {bad_kind}]" if bad_kind else "")


def advance_order(cur: psycopg.Cursor, rng: random.Random) -> str:
    row = random_row(
        cur,
        "select order_id, order_status from olist_app.orders "
        "where order_status in ('created', 'approved', 'invoiced', 'processing', 'shipped') "
        "and deleted_at is null order by random() limit 1",
    )
    if row is None:
        return "advance_order skipped (no open orders)"
    order_id, current = row
    target = next_status(current, rng)
    if target is None:
        return f"advance_order skipped ({current})"

    ts_column = STATUS_TIMESTAMP.get(target)
    if ts_column:
        stmt = sql.SQL(
            "update olist_app.orders set order_status = %s, {col} = now() at time zone 'UTC' "
            "where order_id = %s"
        ).format(col=sql.Identifier(ts_column))
    else:
        stmt = sql.SQL("update olist_app.orders set order_status = %s where order_id = %s")
    cur.execute(stmt, (target, order_id))
    return f"advance_order {order_id} {current} -> {target}"


def add_review(cur: psycopg.Cursor, rng: random.Random, bad: bool) -> str:
    row = random_row(
        cur,
        "select o.order_id from olist_app.orders o "
        "where o.order_status = 'delivered' and o.deleted_at is null and not exists ("
        "select 1 from olist_app.order_reviews r where r.order_id = o.order_id) "
        "order by random() limit 1",
    )
    if row is None:
        return "add_review skipped (no delivered order without review)"
    score = rng.choice([0, 6, 9]) if bad else rng.randint(1, 5)
    cur.execute(
        "insert into olist_app.order_reviews (review_id, order_id, review_score, "
        "review_creation_date) values (%s, %s, %s, now() at time zone 'UTC')",
        (new_id(), row[0], score),
    )
    return f"add_review {row[0]} score={score}" + (" [bad]" if bad else "")


def delete_review(cur: psycopg.Cursor) -> str:
    cur.execute(
        "update olist_app.order_reviews set deleted_at = now() "
        "where (review_id, order_id) = ("
        "select review_id, order_id from olist_app.order_reviews "
        "where deleted_at is null order by random() limit 1) "
        "returning review_id"
    )
    row = cur.fetchone()
    return f"delete_review {row[0]}" if row else "delete_review skipped"


def run_event(conn: psycopg.Connection, rng: random.Random, bad_rate: float) -> str:
    event = pick_event(rng)
    bad = rng.random() < bad_rate
    with conn.cursor() as cur:
        if event == "new_order":
            result = new_order(cur, rng, rng.choice(BAD_KINDS) if bad else None)
        elif event == "advance_order":
            result = advance_order(cur, rng)
        elif event == "add_review":
            result = add_review(cur, rng, bad)
        else:
            result = delete_review(cur)
    conn.commit()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events-per-minute", type=float, default=60)
    parser.add_argument("--max-events", type=int, default=0, help="0 = run until stopped")
    parser.add_argument("--bad-rate", type=float, default=0.0, help="share of bad events, 0-1")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    rng = random.Random(args.seed)
    pause = 60.0 / args.events_per_minute
    count = 0
    with psycopg.connect(pg_conninfo()) as conn:
        try:
            while args.max_events == 0 or count < args.max_events:
                log.info(run_event(conn, rng, args.bad_rate))
                count += 1
                time.sleep(pause)
        except KeyboardInterrupt:
            log.info("Stopped")
    log.info("%d events", count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
