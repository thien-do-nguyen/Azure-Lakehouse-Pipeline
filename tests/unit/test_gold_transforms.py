from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from ecommerce_pipeline.jobs import gold_transforms


def test_build_gold_tables_links_fact_to_temporal_customer_version(monkeypatch, spark, local_config) -> None:
    ts = datetime(2026, 1, 2, 12)
    tables = {
        "app_users": spark.createDataFrame(
            [(1, "u-1", "alice", "alice@example.com", "Alice", "A", "0900", "active", datetime(2026, 1, 1), ts, ts)],
            ["user_id", "public_user_id", "username", "email", "first_name", "last_name", "phone_number", "status", "created_at", "updated_at", "last_login"],
        ),
        "user_addresses": spark.createDataFrame(
            [(1, 1, "shipping", "Alice", "0900", "1 Main", "W", "D", "HCM", "S", "70000", "VN")],
            ["address_id", "user_id", "address_type", "recipient_name", "phone_number", "street", "ward", "district", "city", "state", "postal_code", "country"],
        ),
        "shops": spark.createDataFrame([(1, "s-1", "Shop", "shop", "active", ts)], ["shop_id", "public_shop_id", "shop_name", "shop_slug", "status", "created_at"]),
        "categories": spark.createDataFrame([(1, None, "Category", "category", True, ts)], "category_id long, parent_category_id long, category_name string, slug string, is_active boolean, created_at timestamp"),
        "products": spark.createDataFrame([(1, 1, "p-1", "SKU", "product", "Product", "Brand", "active", True, "{}", "[]", ts)], ["product_id", "category_id", "public_product_id", "product_sku", "product_slug", "product_name", "brand", "status", "is_featured", "attributes_json", "images_json", "created_at"]),
        "product_variants": spark.createDataFrame([(1, 1, "v-1", "VSKU", "Default", "active", "{}", True, Decimal("10.00"), Decimal("12.00"), "VND", 10, 0, Decimal("1.00"), "[]", ts)], ["product_variant_id", "product_id", "public_variant_id", "variant_sku", "variant_name", "status", "options_json", "is_default", "unit_price", "compare_at_price", "currency", "stock_quantity", "reserved_quantity", "weight_kg", "images_json", "created_at"]),
        "vouchers": spark.createDataFrame([(1, "OFF", "Discount", "fixed", "{}", ts, ts, Decimal("0.00"), True)], ["voucher_id", "voucher_code", "voucher_name", "discount_type", "scope_json", "starts_at", "ends_at", "minimum_order_amount", "is_active"]),
        "orders": spark.createDataFrame([(1, 1, 1, 1, "ORD-1", "paid", "paid", Decimal("1.00"), Decimal("2.00"), ts)], ["order_id", "customer_id", "shipping_address_id", "billing_address_id", "order_number", "order_status", "payment_status", "discount_amount", "shipping_amount", "created_at"]),
        "order_items": spark.createDataFrame([(1, 1, 1, 1, 1, "VND", 1, Decimal("10.00"), Decimal("10.00"), Decimal("0.00"), Decimal("1.00"), Decimal("11.00"), ts)], ["order_item_id", "order_id", "product_id", "product_variant_id", "shop_id", "currency", "quantity", "unit_price", "item_subtotal", "discount_amount", "tax_amount", "item_total", "created_at"]),
        "order_vouchers": spark.createDataFrame([(1, 1, 1)], ["order_voucher_id", "order_id", "voucher_id"]),
        "payments": spark.createDataFrame([(1, 1, "visa", "paid")], ["payment_id", "order_id", "payment_method", "payment_status"]),
        "shipments": spark.createDataFrame([(1, 1, "DHL", "delivered", ts, ts)], ["shipment_id", "order_id", "carrier", "shipment_status", "shipped_at", "delivered_at"]),
    }
    monkeypatch.setattr(gold_transforms, "_read_gold_source_table", lambda _spark, _config, table: tables[table])
    history = spark.createDataFrame(
        [(101, 1, datetime(2026, 1, 1), datetime(2026, 2, 1)), (102, 1, datetime(2026, 2, 1), datetime(9999, 12, 31))],
        ["customer_key", "source_customer_id", "start_date", "end_date"],
    )

    gold = gold_transforms.build_gold_tables(local_config, spark, customer_history=history)
    fact = gold["fact_sales"].select("customer_key", "source_order_id", "source_order_item_id").collect()

    assert set(gold) == {
        "dim_date", "dim_time", "dim_customer", "dim_location", "dim_shop", "dim_category",
        "dim_product", "dim_promotion", "dim_payment", "dim_shipping", "fact_sales",
    }
    assert fact[0]["customer_key"] == 101

