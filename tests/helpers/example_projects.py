from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

from structure.app.dsl.api import compile_transform
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.target.pyspark.api import PySpark

ROOT = Path(".")
EXAMPLES = ROOT / "examples"


def render_orders_example() -> dict[str, str]:
    with _example_imports():
        from examples.orders.schemas.analytics import CustomerDailyTotal, ProductDailySummary
        from examples.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
        from examples.orders.schemas.customer import Customer
        from examples.orders.schemas.order import (
            CustomerOrderBackfill,
            OrderCustomerReconciliation,
            OrderFulfillment,
            OrderNormalized,
            OrderProductCandidate,
            OrderPublication,
            OrderPublished,
            OrderRaw,
            OrderWithCustomer,
            OrderWithProduct,
            OrderWithPromotion,
            PublicationFlags,
        )
        from examples.orders.schemas.product import BlockedProduct, Product, ProductBase
        from examples.orders.schemas.promotion import Promotion
        from examples.orders.schemas.shipment import Shipment
        from examples.orders.transforms.analytics import OrderAnalytics
        from examples.orders.transforms.order import EnrichOrders
        from examples.orders.transforms.rowset_join import RowsetJoinExamples

        schema_modules: dict[str, Sequence[type[Structure]]] = {
            "examples.orders.schemas.analytics": [CustomerDailyTotal, ProductDailySummary],
            "examples.orders.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
            "examples.orders.schemas.customer": [Customer],
            "examples.orders.schemas.order": [
                OrderRaw,
                OrderNormalized,
                OrderWithCustomer,
                OrderWithProduct,
                OrderWithPromotion,
                OrderFulfillment,
                OrderPublication,
                PublicationFlags,
                OrderPublished,
                OrderCustomerReconciliation,
                CustomerOrderBackfill,
                OrderProductCandidate,
            ],
            "examples.orders.schemas.product": [ProductBase, Product, BlockedProduct],
            "examples.orders.schemas.promotion": [Promotion],
            "examples.orders.schemas.shipment": [Shipment],
        }
        files = {}
        for transform_class, source_transform in (
            (EnrichOrders, "examples.orders.transforms.order.EnrichOrders"),
            (RowsetJoinExamples, "examples.orders.transforms.rowset_join.RowsetJoinExamples"),
            (OrderAnalytics, "examples.orders.transforms.analytics.OrderAnalytics"),
        ):
            files.update(
                PySpark.render.project()(
                    PySpark.plan.lower()(compile_transform(transform_class)),
                    source_transform=source_transform,
                    generated_package="examples.structure_generated.orders",
                    source_schema_modules=schema_modules,
                )
            )
        files["examples/structure_generated/orders/traceability/__init__.py"] = (
            "# Generated traceability package marker.\n"
        )
        files["examples/structure_generated/orders/traceability/transforms/__init__.py"] = (
            "# Generated transform traceability package marker.\n"
        )
        return files


def expected_orders_generated() -> dict[str, str]:
    root = ROOT
    return {
        str(path.relative_to(root)).replace("\\", "/"): path.read_text(encoding="utf-8")
        for path in sorted((EXAMPLES / "structure_generated").rglob("*"))
        if path.is_file()
    }


@contextmanager
def _example_imports() -> Iterator[None]:
    path = str(ROOT.resolve())
    sys.path.insert(0, path)
    try:
        yield
    finally:
        sys.path.remove(path)
        _drop("examples.orders")
        _drop("examples.structure_generated")


def _drop(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            sys.modules.pop(name, None)
