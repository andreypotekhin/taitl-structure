from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark

EXAMPLES = Path("examples")


def render_orders_example() -> dict[str, str]:
    with _example_imports():
        from orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
        from orders.schemas.customer import Customer
        from orders.schemas.order import (
            OrderNormalized,
            OrderPublication,
            OrderPublished,
            OrderRaw,
            OrderWithCustomer,
            OrderWithProduct,
            OrderWithPromotion,
            PublicationFlags,
        )
        from orders.schemas.product import Product
        from orders.schemas.promotion import Promotion
        from orders.schemas.shipment import Shipment
        from orders.transforms.order import EnrichOrders

        files = PySpark.render.project()(
            PySpark.plan.lower()(compile_transform(EnrichOrders)),
            source_transform="orders.transforms.order.EnrichOrders",
            generated_package="structure_generated.orders",
            source_schema_modules={
                "orders.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
                "orders.schemas.customer": [Customer],
                "orders.schemas.order": [
                    OrderRaw,
                    OrderNormalized,
                    OrderWithCustomer,
                    OrderWithProduct,
                    OrderWithPromotion,
                    OrderPublication,
                    PublicationFlags,
                    OrderPublished,
                ],
                "orders.schemas.product": [Product],
                "orders.schemas.promotion": [Promotion],
                "orders.schemas.shipment": [Shipment],
            },
        )
        files["structure_generated/orders/traceability/__init__.py"] = "# Generated traceability package marker.\n"
        files["structure_generated/orders/traceability/transforms/__init__.py"] = (
            "# Generated transform traceability package marker.\n"
        )
        return files


def expected_orders_generated() -> dict[str, str]:
    root = EXAMPLES
    return {
        str(path.relative_to(root)).replace("\\", "/"): path.read_text(encoding="utf-8")
        for path in sorted((root / "structure_generated").rglob("*"))
        if path.is_file()
    }


@contextmanager
def _example_imports() -> Iterator[None]:
    path = str(EXAMPLES.resolve())
    sys.path.insert(0, path)
    try:
        yield
    finally:
        sys.path.remove(path)
        _drop("orders")


def _drop(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            sys.modules.pop(name, None)
