"""Canonical mapping between the Olist CSV files and Bronze tables."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OlistDataset:
    filename: str
    table: str
    columns: tuple[str, ...]


DATASETS: tuple[OlistDataset, ...] = (
    OlistDataset(
        "olist_customers_dataset.csv",
        "RAW_CUSTOMERS",
        (
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ),
    ),
    OlistDataset(
        "olist_geolocation_dataset.csv",
        "RAW_GEOLOCATION",
        (
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ),
    ),
    OlistDataset(
        "olist_order_items_dataset.csv",
        "RAW_ORDER_ITEMS",
        (
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ),
    ),
    OlistDataset(
        "olist_order_payments_dataset.csv",
        "RAW_ORDER_PAYMENTS",
        (
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ),
    ),
    OlistDataset(
        "olist_order_reviews_dataset.csv",
        "RAW_ORDER_REVIEWS",
        (
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ),
    ),
    OlistDataset(
        "olist_orders_dataset.csv",
        "RAW_ORDERS",
        (
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
    ),
    OlistDataset(
        "olist_products_dataset.csv",
        "RAW_PRODUCTS",
        (
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
    ),
    OlistDataset(
        "olist_sellers_dataset.csv",
        "RAW_SELLERS",
        (
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ),
    ),
    OlistDataset(
        "product_category_name_translation.csv",
        "RAW_PRODUCT_CATEGORY_TRANSLATION",
        ("product_category_name", "product_category_name_english"),
    ),
)


DATASETS_BY_FILENAME = {dataset.filename: dataset for dataset in DATASETS}

