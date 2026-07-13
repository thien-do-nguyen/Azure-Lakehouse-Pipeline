from __future__ import annotations

import argparse
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ecommerce_pipeline.config import load_config


@dataclass(frozen=True)
class Variant:
    product_id: int
    product_variant_id: int
    shop_id: int
    product_name: str
    product_sku: str
    variant_name: str
    variant_sku: str
    unit_price: Decimal


@dataclass(frozen=True)
class RuntimeState:
    user_ids: list[int]
    address_by_user: dict[int, int]
    variants: list[Variant]
    voucher_ids: list[int]


def _money(value: float | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"))


def _execute_returning_id(cur, sql: str, params: tuple) -> int:
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(next(iter(row.values())))


def _reset_source(cur) -> None:
    cur.execute("""
        TRUNCATE TABLE
            customer_app.reviews,
            customer_app.refunds,
            customer_app.returns,
            customer_app.shipments,
            customer_app.payments,
            customer_app.order_vouchers,
            customer_app.order_items,
            customer_app.orders,
            customer_app.vouchers,
            customer_app.cart_items,
            customer_app.shopping_carts,
            customer_app.product_variants,
            customer_app.products,
            customer_app.categories,
            customer_app.shops,
            customer_app.user_addresses,
            customer_app.app_users
        RESTART IDENTITY CASCADE
        """)


def _load_runtime_state(cur) -> RuntimeState:
    cur.execute("SELECT user_id FROM app_users ORDER BY user_id")
    user_ids = [int(row["user_id"]) for row in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT ON (user_id) user_id, address_id
        FROM user_addresses
        ORDER BY user_id, is_default_shipping DESC, address_id
        """)
    address_by_user = {int(row["user_id"]): int(row["address_id"]) for row in cur.fetchall()}

    cur.execute("""
        SELECT
            p.product_id,
            v.product_variant_id,
            p.shop_id,
            p.product_name,
            p.product_sku,
            v.variant_name,
            v.variant_sku,
            v.unit_price
        FROM product_variants v
        JOIN products p ON p.product_id = v.product_id
        WHERE p.status = 'active'
          AND v.status = 'active'
        ORDER BY v.product_variant_id
        """)
    variants = [
        Variant(
            product_id=int(row["product_id"]),
            product_variant_id=int(row["product_variant_id"]),
            shop_id=int(row["shop_id"]),
            product_name=str(row["product_name"]),
            product_sku=str(row["product_sku"]),
            variant_name=str(row["variant_name"]),
            variant_sku=str(row["variant_sku"]),
            unit_price=Decimal(row["unit_price"]),
        )
        for row in cur.fetchall()
    ]

    cur.execute("SELECT voucher_id FROM vouchers WHERE is_active = TRUE ORDER BY voucher_id")
    voucher_ids = [int(row["voucher_id"]) for row in cur.fetchall()]

    if not user_ids or not address_by_user or not variants:
        raise RuntimeError("Seed baseline data first before starting continuous generation.")

    return RuntimeState(
        user_ids=user_ids,
        address_by_user=address_by_user,
        variants=variants,
        voucher_ids=voucher_ids,
    )


def _insert_order(cur, rng: random.Random, state: RuntimeState, order_number: str, created_at: datetime) -> int:
    payment_methods = ["visa", "momo", "vnpay", "cod"]
    carriers = ["GHN", "GHTK", "VNPost", "J&T"]

    customer_id = rng.choice(state.user_ids)
    address_id = state.address_by_user[customer_id]
    selected_items = rng.sample(state.variants, rng.randint(1, min(4, len(state.variants))))
    line_payloads = []
    subtotal = Decimal("0.00")
    line_tax_total = Decimal("0.00")
    for variant in selected_items:
        quantity = rng.randint(1, 3)
        line_discount = _money(variant.unit_price * quantity * Decimal(str(rng.choice([0, 0.03, 0.05]))))
        line_tax = _money(variant.unit_price * quantity * Decimal("0.08"))
        subtotal += _money(variant.unit_price * quantity)
        line_tax_total += line_tax
        line_payloads.append((variant, quantity, line_tax, line_discount))

    shipping_amount = _money(rng.choice([0, 20000, 35000, 50000]))
    order_discount = _money(rng.choice([0, 20000, 50000, 80000]))
    max_discount = subtotal + shipping_amount + line_tax_total
    order_discount = min(order_discount, max_discount)
    total_amount = subtotal + shipping_amount + line_tax_total - order_discount
    payment_status = rng.choice(["paid", "paid", "paid", "pending", "failed"])
    order_status = "confirmed" if payment_status == "paid" else "pending_payment"

    cur.execute("SELECT row_to_json(a) FROM user_addresses a WHERE address_id = %s", (address_id,))
    address_row = cur.fetchone()
    if address_row is None:
        raise RuntimeError(f"Missing address snapshot for address_id={address_id}")
    address_snapshot = address_row["row_to_json"]
    order_id = _execute_returning_id(
        cur,
        """
        INSERT INTO orders (
            customer_id, order_number, order_status, payment_status,
            shipping_address_id, billing_address_id, shipping_address_snapshot,
            billing_address_snapshot, subtotal_amount, shipping_amount,
            tax_amount, discount_amount, currency, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'VND', %s, %s)
        RETURNING order_id
        """,
        (
            customer_id,
            order_number,
            order_status,
            payment_status,
            address_id,
            address_id,
            Jsonb(address_snapshot),
            Jsonb(address_snapshot),
            subtotal,
            shipping_amount,
            line_tax_total,
            order_discount,
            created_at,
            created_at,
        ),
    )

    for variant, quantity, line_tax, line_discount in line_payloads:
        cur.execute(
            """
            INSERT INTO order_items (
                order_id, shop_id, product_id, product_variant_id,
                product_name_snapshot, product_sku_snapshot,
                variant_name_snapshot, variant_sku_snapshot,
                variant_options_snapshot, quantity, unit_price, currency,
                tax_amount, discount_amount, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'VND', %s, %s, %s)
            """,
            (
                order_id,
                variant.shop_id,
                variant.product_id,
                variant.product_variant_id,
                variant.product_name,
                variant.product_sku,
                variant.variant_name,
                variant.variant_sku,
                Jsonb({"tier": variant.variant_name.lower()}),
                quantity,
                variant.unit_price,
                line_tax,
                line_discount,
                created_at,
            ),
        )

    if order_discount > 0 and state.voucher_ids:
        cur.execute(
            """
            INSERT INTO order_vouchers (order_id, voucher_id, discount_amount, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (order_id, rng.choice(state.voucher_ids), order_discount, created_at),
        )

    paid_at = created_at + timedelta(minutes=rng.randint(1, 60)) if payment_status == "paid" else None
    cur.execute(
        """
        INSERT INTO payments (
            order_id, payment_method, payment_status, transaction_reference,
            amount, currency, paid_at, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, 'VND', %s, %s, %s)
        """,
        (
            order_id,
            rng.choice(payment_methods),
            payment_status,
            f"TXN-{order_number}" if payment_status == "paid" else None,
            total_amount,
            paid_at,
            created_at,
            created_at,
        ),
    )

    shipped_at = created_at + timedelta(days=1) if payment_status == "paid" else None
    delivered_at = shipped_at + timedelta(days=rng.randint(1, 5)) if shipped_at and rng.random() > 0.25 else None
    shipment_status = "delivered" if delivered_at else ("in_transit" if shipped_at else "pending")
    cur.execute(
        """
        INSERT INTO shipments (
            order_id, shipment_status, carrier, tracking_number,
            shipping_address_snapshot, shipped_at, delivered_at,
            created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            order_id,
            shipment_status,
            rng.choice(carriers),
            f"TRK-{order_number}" if shipped_at else None,
            Jsonb(address_snapshot),
            shipped_at,
            delivered_at,
            created_at,
            created_at,
        ),
    )
    return order_id


def _touch_existing_rows(cur, rng: random.Random, state: RuntimeState, changed_at: datetime) -> None:
    user_id = rng.choice(state.user_ids)
    cur.execute(
        """
        UPDATE app_users
        SET last_login = %s, updated_at = %s
        WHERE user_id = %s
        """,
        (changed_at, changed_at, user_id),
    )

    variant = rng.choice(state.variants)
    price_multiplier = Decimal(str(rng.choice([0.98, 1.01, 1.03])))
    new_price = _money(variant.unit_price * price_multiplier)
    cur.execute(
        """
        UPDATE product_variants
        SET unit_price = %s, updated_at = %s
        WHERE product_variant_id = %s
        """,
        (new_price, changed_at, variant.product_variant_id),
    )

    cur.execute(
        """
        UPDATE orders
        SET updated_at = %s
        WHERE order_id = (
            SELECT order_id
            FROM orders
            ORDER BY order_id DESC
            LIMIT 1
        )
        """,
        (changed_at,),
    )


def seed(config_path: str, customers: int, orders: int, reset: bool) -> None:
    config = load_config(config_path)
    rng = random.Random(42)
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

    with psycopg.connect(config.postgres.psycopg_dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO customer_app, public")
            if reset:
                _reset_source(cur)

            category_ids: list[int] = []
            for name in ["Electronics", "Fashion", "Home", "Beauty", "Sports"]:
                category_ids.append(
                    _execute_returning_id(
                        cur,
                        """
                        INSERT INTO categories (category_name, slug, description)
                        VALUES (%s, %s, %s)
                        RETURNING category_id
                        """,
                        (name, name.lower(), f"{name} category"),
                    )
                )

            user_ids: list[int] = []
            address_by_user: dict[int, int] = {}
            cities = ["Ho Chi Minh City", "Ha Noi", "Da Nang", "Can Tho", "Hue"]
            for idx in range(1, customers + 1):
                user_id = _execute_returning_id(
                    cur,
                    """
                    INSERT INTO app_users (
                        username, email, password_hash, first_name, last_name,
                        phone_number, status, created_at, updated_at, last_login
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'active', %s, %s, %s)
                    RETURNING user_id
                    """,
                    (
                        f"user{idx:04d}",
                        f"user{idx:04d}@example.com",
                        "synthetic_hash",
                        f"First{idx}",
                        f"Last{idx}",
                        f"090{idx:07d}"[:10],
                        now - timedelta(days=rng.randint(30, 365)),
                        now - timedelta(days=rng.randint(1, 20)),
                        now - timedelta(days=rng.randint(0, 10)),
                    ),
                )
                address_id = _execute_returning_id(
                    cur,
                    """
                    INSERT INTO user_addresses (
                        user_id, address_type, recipient_name, phone_number, street,
                        ward, district, city, country, is_default_shipping,
                        is_default_billing, created_at, updated_at
                    )
                    VALUES (%s, 'shipping', %s, %s, %s, %s, %s, %s, 'Vietnam', TRUE, TRUE, %s, %s)
                    RETURNING address_id
                    """,
                    (
                        user_id,
                        f"First{idx} Last{idx}",
                        f"090{idx:07d}"[:10],
                        f"{idx} Nguyen Trai",
                        f"Ward {rng.randint(1, 12)}",
                        f"District {rng.randint(1, 12)}",
                        rng.choice(cities),
                        now - timedelta(days=rng.randint(30, 365)),
                        now - timedelta(days=rng.randint(1, 20)),
                    ),
                )
                user_ids.append(user_id)
                address_by_user[user_id] = address_id

            shop_ids: list[int] = []
            for name in ["Blue Market", "Urban Goods", "Saigon Style", "Cloud Home"]:
                shop_ids.append(
                    _execute_returning_id(
                        cur,
                        """
                        INSERT INTO shops (shop_name, shop_slug, description, status, created_at, updated_at)
                        VALUES (%s, %s, %s, 'active', %s, %s)
                        RETURNING shop_id
                        """,
                        (name, name.lower().replace(" ", "-"), f"{name} seller", now, now),
                    )
                )

            variants: list[Variant] = []
            product_counter = 1
            for shop_id in shop_ids:
                for _ in range(6):
                    category_id = rng.choice(category_ids)
                    product_id = _execute_returning_id(
                        cur,
                        """
                        INSERT INTO products (
                            shop_id, category_id, product_sku, product_slug, product_name,
                            short_description, brand, attributes_json, images_json,
                            status, is_featured, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, %s, %s)
                        RETURNING product_id
                        """,
                        (
                            shop_id,
                            category_id,
                            f"SKU-{product_counter:04d}",
                            f"product-{product_counter:04d}",
                            f"Product {product_counter:04d}",
                            "Synthetic product for batch pipeline testing",
                            rng.choice(["Aster", "Northline", "Mekong", "Nova"]),
                            Jsonb({"source": "synthetic"}),
                            Jsonb([]),
                            rng.choice([True, False]),
                            now,
                            now,
                        ),
                    )
                    for variant_idx, option in enumerate(["Standard", "Premium"], start=1):
                        price = _money(rng.randint(50_000, 1_500_000))
                        variant_id = _execute_returning_id(
                            cur,
                            """
                            INSERT INTO product_variants (
                                product_id, variant_sku, variant_name, options_json,
                                unit_price, compare_at_price, currency, stock_quantity,
                                reserved_quantity, weight_kg, images_json, status,
                                is_default, created_at, updated_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, 'VND', %s, 0, %s, %s, 'active', %s, %s, %s)
                            RETURNING product_variant_id
                            """,
                            (
                                product_id,
                                f"SKU-{product_counter:04d}-{variant_idx}",
                                option,
                                Jsonb({"tier": option.lower()}),
                                price,
                                _money(price * Decimal("1.15")),
                                rng.randint(20, 500),
                                _money(rng.uniform(0.1, 3.0)),
                                Jsonb([]),
                                variant_idx == 1,
                                now,
                                now,
                            ),
                        )
                        variants.append(
                            Variant(
                                product_id=product_id,
                                product_variant_id=variant_id,
                                shop_id=shop_id,
                                product_name=f"Product {product_counter:04d}",
                                product_sku=f"SKU-{product_counter:04d}",
                                variant_name=option,
                                variant_sku=f"SKU-{product_counter:04d}-{variant_idx}",
                                unit_price=price,
                            )
                        )
                    product_counter += 1

            voucher_ids: list[int] = []
            for code, value in [("WELCOME50", 50_000), ("SALE100", 100_000), ("VIP5PCT", 5)]:
                discount_type = "percent" if code.endswith("PCT") else "fixed"
                voucher_ids.append(
                    _execute_returning_id(
                        cur,
                        """
                        INSERT INTO vouchers (
                            voucher_code, voucher_name, discount_type, discount_value,
                            max_discount_amount, minimum_order_amount, scope_json,
                            starts_at, ends_at, usage_limit, is_active, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, 0, %s, %s, %s, 1000, TRUE, %s, %s)
                        RETURNING voucher_id
                        """,
                        (
                            code,
                            code,
                            discount_type,
                            _money(value),
                            _money(80_000) if discount_type == "percent" else None,
                            Jsonb({"type": "all"}),
                            now - timedelta(days=30),
                            now + timedelta(days=60),
                            now,
                            now,
                        ),
                    )
                )

            payment_methods = ["visa", "momo", "vnpay", "cod"]
            carriers = ["GHN", "GHTK", "VNPost", "J&T"]
            for order_idx in range(1, orders + 1):
                customer_id = rng.choice(user_ids)
                address_id = address_by_user[customer_id]
                created_at = now - timedelta(
                    days=rng.randint(0, 90), hours=rng.randint(0, 23), minutes=rng.randint(0, 59)
                )
                selected_items = rng.sample(variants, rng.randint(1, 4))
                line_payloads = []
                subtotal = Decimal("0.00")
                line_tax_total = Decimal("0.00")
                for variant in selected_items:
                    quantity = rng.randint(1, 3)
                    line_discount = _money(variant.unit_price * quantity * Decimal(str(rng.choice([0, 0.03, 0.05]))))
                    line_tax = _money(variant.unit_price * quantity * Decimal("0.08"))
                    subtotal += _money(variant.unit_price * quantity)
                    line_tax_total += line_tax
                    line_payloads.append((variant, quantity, line_tax, line_discount))

                shipping_amount = _money(rng.choice([0, 20000, 35000, 50000]))
                order_discount = _money(rng.choice([0, 20000, 50000, 80000]))
                max_discount = subtotal + shipping_amount + line_tax_total
                order_discount = min(order_discount, max_discount)
                total_amount = subtotal + shipping_amount + line_tax_total - order_discount
                payment_status = rng.choice(["paid", "paid", "paid", "pending", "failed"])
                order_status = "confirmed" if payment_status == "paid" else "pending_payment"

                cur.execute("SELECT row_to_json(a) FROM user_addresses a WHERE address_id = %s", (address_id,))
                address_row = cur.fetchone()
                if address_row is None:
                    raise RuntimeError(f"Missing address snapshot for address_id={address_id}")
                address_snapshot = address_row["row_to_json"]
                order_id = _execute_returning_id(
                    cur,
                    """
                    INSERT INTO orders (
                        customer_id, order_number, order_status, payment_status,
                        shipping_address_id, billing_address_id, shipping_address_snapshot,
                        billing_address_snapshot, subtotal_amount, shipping_amount,
                        tax_amount, discount_amount, currency, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'VND', %s, %s)
                    RETURNING order_id
                    """,
                    (
                        customer_id,
                        f"ORD-{order_idx:06d}",
                        order_status,
                        payment_status,
                        address_id,
                        address_id,
                        Jsonb(address_snapshot),
                        Jsonb(address_snapshot),
                        subtotal,
                        shipping_amount,
                        line_tax_total,
                        order_discount,
                        created_at,
                        created_at,
                    ),
                )

                for variant, quantity, line_tax, line_discount in line_payloads:
                    cur.execute(
                        """
                        INSERT INTO order_items (
                            order_id, shop_id, product_id, product_variant_id,
                            product_name_snapshot, product_sku_snapshot,
                            variant_name_snapshot, variant_sku_snapshot,
                            variant_options_snapshot, quantity, unit_price, currency,
                            tax_amount, discount_amount, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'VND', %s, %s, %s)
                        """,
                        (
                            order_id,
                            variant.shop_id,
                            variant.product_id,
                            variant.product_variant_id,
                            variant.product_name,
                            variant.product_sku,
                            variant.variant_name,
                            variant.variant_sku,
                            Jsonb({"tier": variant.variant_name.lower()}),
                            quantity,
                            variant.unit_price,
                            line_tax,
                            line_discount,
                            created_at,
                        ),
                    )

                if order_discount > 0:
                    cur.execute(
                        """
                        INSERT INTO order_vouchers (order_id, voucher_id, discount_amount, created_at)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (order_id, rng.choice(voucher_ids), order_discount, created_at),
                    )

                paid_at = created_at + timedelta(minutes=rng.randint(1, 60)) if payment_status == "paid" else None
                cur.execute(
                    """
                    INSERT INTO payments (
                        order_id, payment_method, payment_status, transaction_reference,
                        amount, currency, paid_at, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, 'VND', %s, %s, %s)
                    """,
                    (
                        order_id,
                        rng.choice(payment_methods),
                        payment_status,
                        f"TXN-{order_idx:06d}" if payment_status == "paid" else None,
                        total_amount,
                        paid_at,
                        created_at,
                        created_at,
                    ),
                )

                shipped_at = created_at + timedelta(days=1) if payment_status == "paid" else None
                delivered_at = (
                    shipped_at + timedelta(days=rng.randint(1, 5)) if shipped_at and rng.random() > 0.25 else None
                )
                shipment_status = "delivered" if delivered_at else ("in_transit" if shipped_at else "pending")
                cur.execute(
                    """
                    INSERT INTO shipments (
                        order_id, shipment_status, carrier, tracking_number,
                        shipping_address_snapshot, shipped_at, delivered_at,
                        created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        order_id,
                        shipment_status,
                        rng.choice(carriers),
                        f"TRK-{order_idx:06d}" if shipped_at else None,
                        Jsonb(address_snapshot),
                        shipped_at,
                        delivered_at,
                        created_at,
                        created_at,
                    ),
                )

        conn.commit()


def seed_continuous(
    config_path: str,
    orders_per_batch: int,
    interval_seconds: float,
    max_batches: int | None,
    reset: bool,
    bootstrap_customers: int,
    bootstrap_orders: int,
) -> None:
    if orders_per_batch < 1:
        raise ValueError("orders_per_batch must be at least 1")
    if interval_seconds < 0:
        raise ValueError("interval_seconds must be greater than or equal to 0")
    if max_batches is not None and max_batches < 1:
        raise ValueError("max_batches must be at least 1 when provided")

    if reset:
        seed(config_path, bootstrap_customers, bootstrap_orders, reset=True)

    config = load_config(config_path)
    rng = random.Random()
    batch_number = 0
    print(
        "continuous seed started "
        f"(orders_per_batch={orders_per_batch}, interval_seconds={interval_seconds}, max_batches={max_batches})",
        flush=True,
    )

    while max_batches is None or batch_number < max_batches:
        batch_number += 1
        now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
        with psycopg.connect(config.postgres.psycopg_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SET search_path TO customer_app, public")
                state = _load_runtime_state(cur)
                order_ids = []
                stamp = now.strftime("%Y%m%d%H%M%S")
                for idx in range(1, orders_per_batch + 1):
                    order_number = f"ORD-RT-{stamp}-{batch_number:04d}-{idx:03d}"
                    order_ids.append(_insert_order(cur, rng, state, order_number, now))
                _touch_existing_rows(cur, rng, state, now)
            conn.commit()
        print(
            f"batch={batch_number} inserted_orders={len(order_ids)} "
            f"first_order_id={order_ids[0]} last_order_id={order_ids[-1]} changed_at={now.isoformat()}",
            flush=True,
        )
        if max_batches is None or batch_number < max_batches:
            time.sleep(interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic OLTP data.")
    parser.add_argument("--config", default="configs/local.yaml")
    parser.add_argument("--customers", type=int, default=100)
    parser.add_argument("--orders", type=int, default=500)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--continuous", action="store_true", help="Keep inserting small batches for CDC demos.")
    parser.add_argument("--orders-per-batch", type=int, default=5)
    parser.add_argument("--interval-seconds", type=float, default=10.0)
    parser.add_argument("--max-batches", type=int)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.continuous:
        seed_continuous(
            args.config,
            args.orders_per_batch,
            args.interval_seconds,
            args.max_batches,
            args.reset,
            args.customers,
            args.orders,
        )
    else:
        seed(args.config, args.customers, args.orders, args.reset)


if __name__ == "__main__":
    main()
