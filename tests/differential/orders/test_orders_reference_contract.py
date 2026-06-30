from __future__ import annotations

from decimal import Decimal

from helpers.example_projects import render_orders_example


def test_orders_example_matches_independent_reference_rows() -> None:
    rows = _reference_enrich_orders(
        orders=[
            _order(" O-1 ", " C-1 ", " P-1 ", " SUMMER ", "1250.50", "10.00", 2),
            _order("bad", " C-1 ", "missing", "", "8.00", "0.00", None),
        ],
        customers=[{"tenant": {"tenant_id": "t1"}, "id": "c-1", "name": "Ada Lovelace", "tier": "gold"}],
        products=[{"tenant": {"tenant_id": "t1"}, "id": "p-1", "name": "Analytical Engine", "category": "compute"}],
        promotions=[{"tenant": {"tenant_id": "t1"}, "code": "summer", "name": "Summer"}],
    )

    assert rows == [
        {
            "tenant": {"tenant_id": "t1"},
            "business": {"order_date": "2026-01-02"},
            "id": "o-1",
            "customer_id": "c-1",
            "customer_name": "Ada Lovelace",
            "customer_tier": "gold",
            "product_name": "Analytical Engine",
            "product_category": "compute",
            "promotion_name": "Summer",
            "total": Decimal("1250.50"),
            "discount": Decimal("10.00"),
            "net_total": Decimal("1240.50"),
            "quantity": 2,
            "is_large": True,
            "has_promotion": True,
        }
    ]


def test_orders_generated_code_matches_independent_reference_operations() -> None:
    transform = render_orders_example()["examples/structure_generated/orders/pyspark/transforms/order.py"]

    reference_fragments = [
        'assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")',
        'assert_schema(customers, CUSTOMER_SCHEMA, name="Customer", mode="strict")',
        'assert_schema(products, PRODUCT_SCHEMA, name="Product", mode="strict")',
        'assert_schema(promotions, PROMOTION_SCHEMA, name="Promotion", mode="strict")',
        'orders = orders.where((F.col("order_raw.id").isNotNull())',
        'F.coalesce(F.col("order_raw.total").cast("decimal(12,2)"), F.lit(0)).alias("total")',
        'customers_joined = F.broadcast(customers.alias("customers"))',
        'products_joined = products.alias("products")',
        'promotions_joined = promotions.alias("promotions")',
        'published = self._impl.add_quality_columns(published=published, spark=self.spark, ctx=self.ctx)',
        'assert_schema(published, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="strict")',
    ]

    for fragment in reference_fragments:
        assert fragment in transform


def _reference_enrich_orders(
    *,
    orders: list[dict[str, object]],
    customers: list[dict[str, object]],
    products: list[dict[str, object]],
    promotions: list[dict[str, object]],
) -> list[dict[str, object]]:
    published = []
    for raw in orders:
        if not raw["id"] or not raw["customer_id"] or not raw["product_id"]:
            continue

        total = Decimal(str(raw["total"] or "0"))
        discount = Decimal(str(raw["discount"] or "0"))
        net_total = total - discount
        if net_total < 0:
            continue

        tenant = raw["tenant"]
        order = {
            "tenant": tenant,
            "business": raw["business"],
            "id": _clean(raw["id"]),
            "customer_id": _clean(raw["customer_id"]),
            "product_id": _clean(raw["product_id"]),
            "promotion_code": _clean(raw["promotion_code"]),
            "total": total,
            "discount": discount,
            "net_total": net_total,
            "quantity": raw["quantity"] or 1,
            "is_large": total > 1000,
        }
        customer = _find(customers, tenant=tenant, key="id", value=order["customer_id"], clean=True)
        product = _find(products, tenant=tenant, key="id", value=order["product_id"])
        if product is None:
            continue
        promotion = _find(promotions, tenant=tenant, key="code", value=order["promotion_code"], clean=True)

        published.append(
            {
                "tenant": tenant,
                "business": order["business"],
                "id": order["id"],
                "customer_id": order["customer_id"],
                "customer_name": customer["name"] if customer else None,
                "customer_tier": customer["tier"] if customer else None,
                "product_name": product["name"],
                "product_category": product["category"],
                "promotion_name": promotion["name"] if promotion else None,
                "total": total,
                "discount": discount,
                "net_total": net_total,
                "quantity": order["quantity"],
                "is_large": order["is_large"],
                "has_promotion": promotion is not None,
            }
        )
    return published


def _order(
    id: str,
    customer_id: str,
    product_id: str,
    promotion_code: str,
    total: str,
    discount: str,
    quantity: int | None,
) -> dict[str, object]:
    return {
        "tenant": {"tenant_id": "t1"},
        "business": {"order_date": "2026-01-02"},
        "id": id,
        "customer_id": customer_id,
        "product_id": product_id,
        "promotion_code": promotion_code,
        "total": total,
        "discount": discount,
        "quantity": quantity,
    }


def _clean(value: object) -> str:
    return str(value).strip().lower()


def _find(
    rows: list[dict[str, object]],
    *,
    tenant: object,
    key: str,
    value: object,
    clean: bool = False,
) -> dict[str, object] | None:
    for row in rows:
        candidate = _clean(row[key]) if clean else row[key]
        if row["tenant"] == tenant and candidate == value:
            return row
    return None
