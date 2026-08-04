from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_projects, session
from integration.pyspark.support.rows import single

from examples.store.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
from examples.store.schemas.customer import Customer
from examples.store.schemas.fulfillment.demand.demand import Order
from examples.store.schemas.fulfillment.inventory.inventory import InboundInventory, InventoryPosition, LeadTime
from examples.store.schemas.fulfillment.planning.plan import (
    FulfillmentAllocation,
    FulfillmentBackorder,
    FulfillmentPlan,
    ReplenishmentSuggestion,
)
from examples.store.schemas.fulfillment.planning.workflow import (
    FulfillmentOption,
    FulfillmentPreferredOption,
    InboundInventoryAvailability,
)
from examples.store.schemas.fulfillment.warehouses.warehouse import Warehouse
from examples.store.schemas.order import (
    OrderFulfillment,
    OrderNormalized,
    OrderPublication,
    OrderPublished,
    OrderRaw,
    OrderWithCustomer,
    OrderWithProduct,
    OrderWithPromotion,
    PublicationFlags,
)
from examples.store.schemas.product import BlockedProduct, Product, ProductBase
from examples.store.schemas.promotion import Promotion
from examples.store.transforms.fulfillment.demand.prepare import PrepareOrderDemand
from examples.store.transforms.fulfillment.planning.plan import PlanFulfillment
from structure.core.dsl.model.schemas.Schema import Schema

pytestmark = pytest.mark.integration

PACKAGE = "integration_store_generated"
FIXTURES = Path(__file__).resolve().parents[4] / "examples" / "store" / "fixtures"
SCHEMA_MODULES: Mapping[str, Sequence[type[Schema]]] = {
    "examples.store.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
    "examples.store.schemas.customer": [Customer],
    "examples.store.schemas.order": [
        OrderRaw,
        OrderNormalized,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
        OrderFulfillment,
        OrderPublication,
        PublicationFlags,
        OrderPublished,
    ],
    "examples.store.schemas.product": [ProductBase, Product, BlockedProduct],
    "examples.store.schemas.promotion": [Promotion],
    "examples.store.schemas.fulfillment.demand.demand": [Order],
    "examples.store.schemas.fulfillment.inventory.inventory": [InboundInventory, InventoryPosition, LeadTime],
    "examples.store.schemas.fulfillment.warehouses.warehouse": [Warehouse],
    "examples.store.schemas.fulfillment.planning.workflow": [
        InboundInventoryAvailability,
        FulfillmentOption,
        FulfillmentPreferredOption,
    ],
    "examples.store.schemas.fulfillment.planning.plan": [
        FulfillmentAllocation,
        FulfillmentBackorder,
        FulfillmentPlan,
        ReplenishmentSuggestion,
    ],
}


def test_store_fixtures_match_online_and_generated_planning_for_selected_rows(spark, tmp_path) -> None:
    files = render_generated_projects(
        (
            (PrepareOrderDemand, "examples.store.transforms.fulfillment.demand.prepare.PrepareOrderDemand"),
            (PlanFulfillment, "examples.store.transforms.fulfillment.planning.plan.PlanFulfillment"),
        ),
        generated_package=PACKAGE,
        source_schema_modules=SCHEMA_MODULES,
    )

    with generated_project(tmp_path, PACKAGE, files):
        schemas = {
            name: import_module(f"{PACKAGE}.pyspark.schemas.{name}")
            for name in ("order", "customer", "product", "promotion", "demand", "inventory", "warehouse")
        }
        inputs = {
            "orders": spark.createDataFrame(_orders(), schemas["order"].ORDER_RAW_SCHEMA),
            "customers": spark.createDataFrame(_customers(), schemas["customer"].CUSTOMER_SCHEMA),
            "products": spark.createDataFrame(_products(), schemas["product"].PRODUCT_SCHEMA),
            "blocked_products": spark.createDataFrame(_blocked_products(), schemas["product"].BLOCKED_PRODUCT_SCHEMA),
            "promotions": spark.createDataFrame(_promotions(), schemas["promotion"].PROMOTION_SCHEMA),
            "warehouses": spark.createDataFrame(_warehouses(), schemas["warehouse"].WAREHOUSE_SCHEMA),
            "inventory_positions": spark.createDataFrame(
                _inventory_positions(), schemas["inventory"].INVENTORY_POSITION_SCHEMA
            ),
            "inbound_inventory": spark.createDataFrame(
                _inbound_inventory(), schemas["inventory"].INBOUND_INVENTORY_SCHEMA
            ),
        }

        online_demand = PrepareOrderDemand(**inputs).run(session(spark, execution_mode="online")).demand
        generated_demand = (
            PrepareOrderDemand(**inputs)
            .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
            .demand
        )
        for key in (("o-100", 1), ("o-101", 1), ("o-102", 1)):
            online = _selected(online_demand, key)
            generated = _selected(generated_demand, key)
            assert (generated["product_id"], generated["requested_quantity"]) == (
                online["product_id"],
                online["requested_quantity"],
            )

        planning_inputs = {
            "warehouses": inputs["warehouses"],
            "inventory_positions": inputs["inventory_positions"],
            "inbound_inventory": inputs["inbound_inventory"],
        }
        online_plans = (
            PlanFulfillment(demand=online_demand, **planning_inputs).run(session(spark, execution_mode="online")).plans
        )
        generated_plans = (
            PlanFulfillment(demand=generated_demand, **planning_inputs)
            .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
            .plans
        )

        expected_statuses = {
            ("o-100", 1): "allocated",
            ("o-101", 1): "partially_allocated",
            ("o-102", 1): "backordered",
        }
        for key, status in expected_statuses.items():
            online = _selected(online_plans, key)
            generated = _selected(generated_plans, key)
            assert online["plan_status"] == generated["plan_status"] == status
            assert online["allocated_quantity"] == generated["allocated_quantity"]
            assert online["backordered_quantity"] == generated["backordered_quantity"]


def _csv(name: str) -> list[dict[str, str]]:
    with (FIXTURES / name).open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


def _selected(frame: Any, key: tuple[str, int]) -> dict[str, object]:
    return single(frame, lambda row: (row["order_id"], row["line_number"]) == key)


def _tenant(row: dict[str, str]) -> dict[str, str]:
    return {"tenant_id": row["tenant_id"]}


def _audit(row: dict[str, str]) -> dict[str, Any]:
    return {"source_system": row["source_system"], "ingested_at": _timestamp(row["ingested_at"])}


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _optional(value: str) -> str | None:
    return value or None


def _orders() -> list[dict[str, Any]]:
    result = []
    for row in _csv("orders.csv"):
        shipping = None
        if row["shipping_line1"]:
            shipping = {
                "line1": row["shipping_line1"],
                "line2": _optional(row["shipping_line2"]),
                "city": row["shipping_city"],
                "state": _optional(row["shipping_state"]),
                "postal_code": row["shipping_postal_code"],
                "country": row["shipping_country"],
            }
        result.append(
            {
                "tenant": _tenant(row),
                "audit": _audit(row),
                "business": {"order_date": date.fromisoformat(row["order_date"])},
                "id": row["id"],
                "line_number": int(row["line_number"]),
                "customer_id": row["customer_id"],
                "product_id": row["product_id"],
                "promo-code": _optional(row["promotion_code"]),
                "total": _optional(row["total"]),
                "discount": _optional(row["discount"]),
                "quantity": int(row["quantity"]) if row["quantity"] else None,
                "tags": row["tags"].split(";") if row["tags"] else None,
                "attributes": dict(item.split("=", 1) for item in row["attributes"].split(";") if item),
                "shipping": shipping,
            }
        )
    return result


def _customers() -> list[dict[str, Any]]:
    return [
        {
            "tenant": _tenant(row),
            "audit": _audit(row),
            "id": row["id"],
            "name": row["name"],
            "tier": row["tier"],
            "region": row["region"],
            "email": row["email"],
        }
        for row in _csv("customers.csv")
    ]


def _product(row: dict[str, str], *, reason: str | None = None) -> dict[str, Any]:
    product = {
        "tenant": _tenant(row),
        "audit": _audit(row),
        "id": row["id"],
        "name": row["name"],
        "category": row["category"],
        "features": row["features"].split(";") if row["features"] else None,
        "active": row["active"].lower() == "true",
        "list_price": Decimal(row["list_price"]) if row["list_price"] else None,
        "weight": float(row["weight"]) if row["weight"] else None,
        "rating": float(row["rating"]) if row["rating"] else None,
    }
    if reason is not None:
        product["reason"] = reason
    return product


def _products() -> list[dict[str, Any]]:
    return [_product(row) for row in _csv("products.csv")]


def _blocked_products() -> list[dict[str, Any]]:
    return [_product(row, reason=row["reason"]) for row in _csv("blocked_products.csv")]


def _promotions() -> list[dict[str, Any]]:
    return [
        {
            "tenant": _tenant(row),
            "audit": _audit(row),
            "code": row["code"],
            "name": row["name"],
            "discount": Decimal(row["discount"]) if row["discount"] else None,
            "valid_from": date.fromisoformat(row["valid_from"]),
            "valid_to": date.fromisoformat(row["valid_to"]) if row["valid_to"] else None,
        }
        for row in _csv("promotions.csv")
    ]


def _warehouses() -> list[dict[str, Any]]:
    return [
        {
            "tenant": _tenant(row),
            "audit": _audit(row),
            "id": row["id"],
            "name": row["name"],
            "region": row["region"],
            "priority": int(row["priority"]),
            "active": row["active"].lower() == "true",
        }
        for row in _csv("warehouses.csv")
    ]


def _inventory_positions() -> list[dict[str, Any]]:
    return [
        {
            "tenant": _tenant(row),
            "audit": _audit(row),
            "warehouse_id": row["warehouse_id"],
            "product_id": row["product_id"],
            "on_hand_quantity": int(row["on_hand_quantity"]),
            "reserved_quantity": int(row["reserved_quantity"]),
            "safety_stock_quantity": int(row["safety_stock_quantity"]),
            "as_of": date.fromisoformat(row["as_of"]),
        }
        for row in _csv("inventory_positions.csv")
    ]


def _inbound_inventory() -> list[dict[str, Any]]:
    return [
        {
            "tenant": _tenant(row),
            "audit": _audit(row),
            "warehouse_id": row["warehouse_id"],
            "product_id": row["product_id"],
            "expected_quantity": int(row["expected_quantity"]),
            "expected_at": date.fromisoformat(row["expected_at"]) if row["expected_at"] else None,
            "source_type": row["source_type"],
        }
        for row in _csv("inbound_inventory.csv")
    ]
