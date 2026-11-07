"""Source tables handled by the pipeline: CSV file name and primary key columns."""

TABLES: dict[str, dict] = {
    "customers": {
        "csv": "olist_customers_dataset.csv",
        "pk": ["customer_id"],
    },
    "sellers": {
        "csv": "olist_sellers_dataset.csv",
        "pk": ["seller_id"],
    },
    "products": {
        "csv": "olist_products_dataset.csv",
        "pk": ["product_id"],
    },
    "product_category_name_translation": {
        "csv": "product_category_name_translation.csv",
        "pk": ["product_category_name"],
    },
    "orders": {
        "csv": "olist_orders_dataset.csv",
        "pk": ["order_id"],
    },
    "order_items": {
        "csv": "olist_order_items_dataset.csv",
        "pk": ["order_id", "order_item_id"],
    },
    "order_payments": {
        "csv": "olist_order_payments_dataset.csv",
        "pk": ["order_id", "payment_sequential"],
    },
    "order_reviews": {
        "csv": "olist_order_reviews_dataset.csv",
        "pk": ["review_id", "order_id"],
    },
}
