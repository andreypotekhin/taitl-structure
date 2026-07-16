from __future__ import annotations

import csv
import importlib
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from integration.pyspark.support.backend_matrix import session

ROOT = Path(__file__).resolve().parents[5]
DATA = ROOT / "res" / "testing" / "data" / "v1" / "orders"


def source_schema_modules():
    from testing.model.v1.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
    from testing.model.v1.orders.schemas.customer import Customer
    from testing.model.v1.orders.schemas.order import (
        OrderNormalized,
        OrderPublication,
        OrderPublished,
        OrderRaw,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
        PublicationFlags,
    )
    from testing.model.v1.orders.schemas.product import Product
    from testing.model.v1.orders.schemas.promotion import Promotion

    return {
        "testing.model.v1.orders.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
        "testing.model.v1.orders.schemas.customer": [Customer],
        "testing.model.v1.orders.schemas.order": [
            OrderRaw,
            OrderNormalized,
            OrderWithCustomer,
            OrderWithProduct,
            OrderWithPromotion,
            OrderPublication,
            PublicationFlags,
            OrderPublished,
        ],
        "testing.model.v1.orders.schemas.product": [Product],
        "testing.model.v1.orders.schemas.promotion": [Promotion],
    }


def generated_order_schemas(package: str) -> SimpleNamespace:
    order = importlib.import_module(f"{package}.pyspark.schemas.order")
    customer = importlib.import_module(f"{package}.pyspark.schemas.customer")
    product = importlib.import_module(f"{package}.pyspark.schemas.product")
    promotion = importlib.import_module(f"{package}.pyspark.schemas.promotion")
    return SimpleNamespace(
        ORDER_RAW_SCHEMA=order.ORDER_RAW_SCHEMA,
        ORDER_PUBLISHED_SCHEMA=order.ORDER_PUBLISHED_SCHEMA,
        CUSTOMER_SCHEMA=customer.CUSTOMER_SCHEMA,
        PRODUCT_SCHEMA=product.PRODUCT_SCHEMA,
        PROMOTION_SCHEMA=promotion.PROMOTION_SCHEMA,
    )


def run_generated_transform(spark, generated_package: str, schemas):
    invocation = transform()(**input_frames(spark, schemas))
    return _published(invocation.run(session(spark, execution_mode="generated", generated_package=generated_package)))


def run_online_transform(spark, schemas):
    invocation = transform()(**input_frames(spark, schemas))
    return _published(invocation.run(session(spark, execution_mode="online")))


def input_frames(spark, schemas) -> dict[str, object]:
    return {
        "orders": spark.createDataFrame(_rows("orders.csv", _order_converters()), schema=schemas.ORDER_RAW_SCHEMA),
        "customers": spark.createDataFrame(
            _rows("customers.csv", _customer_converters()),
            schema=schemas.CUSTOMER_SCHEMA,
        ),
        "products": spark.createDataFrame(_rows("products.csv", _product_converters()), schema=schemas.PRODUCT_SCHEMA),
        "promotions": spark.createDataFrame(
            _rows("promotions.csv", _promotion_converters()),
            schema=schemas.PROMOTION_SCHEMA,
        ),
    }


def transform():
    from testing.model.v1.orders.transforms.order import EnrichOrders

    return EnrichOrders


def _published(result):
    if hasattr(result, "as_dict"):
        return result["published"]
    return result


def _rows(name: str, converters: dict[str, Callable[[str], object]]) -> list[dict[str, object]]:
    with (DATA / name).open(newline="", encoding="utf-8") as file:
        return [_row(row, converters) for row in csv.DictReader(file)]


def _row(raw: dict[str, str], converters: dict[str, Callable[[str], object]]) -> dict[str, object]:
    row: dict[str, object] = {}
    for key, text in raw.items():
        value = converters.get(key, _nullable_text)(text)
        _assign(row, key.split("."), value)
    _null_struct(row, "shipping")
    _null_struct(row, "attributes")
    return row


def _assign(row: dict[str, object], path: list[str], value: object) -> None:
    target = row
    for part in path[:-1]:
        target = target.setdefault(part, {})  # type: ignore[assignment]
    target[path[-1]] = value


def _null_struct(row: dict[str, object], key: str) -> None:
    value = row.get(key)
    if isinstance(value, dict) and all(item is None for item in value.values()):
        row[key] = None


def _order_converters() -> dict[str, Callable[[str], object]]:
    return {
        "audit.ingested_at": _timestamp,
        "business.order_date": _date,
        "quantity": _nullable_int,
        "tags": _tags,
    }


def _customer_converters() -> dict[str, Callable[[str], object]]:
    return {"audit.ingested_at": _timestamp}


def _product_converters() -> dict[str, Callable[[str], object]]:
    return {
        "audit.ingested_at": _timestamp,
        "active": _bool,
        "list_price": _decimal,
        "weight": _float,
        "rating": _float,
    }


def _promotion_converters() -> dict[str, Callable[[str], object]]:
    return {
        "audit.ingested_at": _timestamp,
        "discount": _decimal,
    }


def _nullable_text(text: str) -> str | None:
    return text if text != "" else None


def _timestamp(text: str) -> datetime | None:
    return datetime.fromisoformat(text) if text else None


def _date(text: str) -> date | None:
    return date.fromisoformat(text) if text else None


def _nullable_int(text: str) -> int | None:
    return int(text) if text else None


def _bool(text: str) -> bool | None:
    return text.lower() == "true" if text else None


def _decimal(text: str) -> Decimal | None:
    return Decimal(text) if text else None


def _float(text: str) -> float | None:
    return float(text) if text else None


def _tags(text: str) -> list[str] | None:
    return text.split("|") if text else None
