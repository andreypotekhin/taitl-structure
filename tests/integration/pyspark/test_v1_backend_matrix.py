from __future__ import annotations

import csv
import importlib
import os
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable

import pytest
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
from testing.model.v1.orders.transforms.order import EnrichOrders

from structure import (
    Join,
    String,
    Structure,
    StructureSession,
    Transform,
    after,
    field,
    input,
    join_one,
    output,
    transform,
)
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "res" / "testing" / "data" / "v1" / "orders"


class LookupOrder(Structure):
    id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)


class LookupProduct(Structure):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=False)


class LookupEnriched(Structure):
    id = field(String(), nullable=False)
    product_name = field(String(), nullable=True)


@transform
class AddLookupProduct(Transform):
    orders = input(LookupOrder)
    products = input(LookupProduct)
    accepted = output(LookupEnriched)
    audited = output(LookupEnriched)

    @transform(inputs=[orders, products], outputs=[accepted, audited])
    def add_product(
        self,
        order: LookupOrder,
        product: LookupProduct,
    ) -> tuple[LookupEnriched, LookupEnriched]:
        product = join_one(
            product,
            on=product.id == order.product_id,
            how=Join.LEFT,
        )
        row = LookupEnriched(id=order.id, product_name=product.name)
        return row, row

    @after(add_product, df=audited)
    def audit(self, *, df, spark, ctx):
        return df


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    sql = pytest.importorskip("pyspark.sql")
    master = os.environ.get("STRUCTURE_SPARK_MASTER", "local[2]")
    builder = (
        sql.SparkSession.builder.master(master)
        .appName(f"structure-integration-{os.environ.get('STRUCTURE_INTEGRATION_BACKEND', 'local')}")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )

    session = None
    last_error = None
    for _ in range(12):
        try:
            session = builder.getOrCreate()
            session.range(1).count()
            break
        except Exception as error:  # pragma: no cover - only exercised while Spark starts.
            last_error = error
            if session is not None:
                session.stop()
            time.sleep(2)

    if session is None:
        raise AssertionError(f"Spark did not become ready at {master}: {last_error}")

    try:
        yield session
    finally:
        session.stop()
        pyspark.SparkContext._active_spark_context = None


def test_v1_backend_runtime_versions(spark) -> None:
    pyspark = pytest.importorskip("pyspark")
    backend = os.environ.get("STRUCTURE_INTEGRATION_BACKEND")
    expected_pyspark = os.environ.get("STRUCTURE_EXPECTED_PYSPARK")
    expected_spark = os.environ.get("STRUCTURE_EXPECTED_SPARK")

    assert backend in {"pyspark35", "pyspark40"}
    assert expected_pyspark is not None
    assert expected_spark is not None
    assert pyspark.__version__.startswith(expected_pyspark)
    assert spark.version.startswith(expected_spark)


def test_v1_online_and_generated_execution_match_orders_contract_on_live_backend(spark, tmp_path) -> None:
    generated_package = "integration_generated"
    files = _render_generated_project(generated_package)
    _write_files(tmp_path, files)

    sys.path.insert(0, str(tmp_path))
    try:
        importlib.invalidate_caches()
        schemas = importlib.import_module(f"{generated_package}.pyspark.schemas.order")
        generated = _run_generated_orders_transform(spark, generated_package, schemas)
        online = _run_online_orders_transform(spark, schemas)

        assert generated.columns == schemas.ORDER_PUBLISHED_SCHEMA.fieldNames()
        assert online.columns == schemas.ORDER_PUBLISHED_SCHEMA.fieldNames()
        generated_rows = [row.asDict(recursive=True) for row in generated.orderBy("id").collect()]
        online_rows = [row.asDict(recursive=True) for row in online.orderBy("id").collect()]
        assert online_rows == generated_rows
        assert generated_rows == [
            {
                "tenant": {"tenant_id": "t1"},
                "business": {"order_date": date(2026, 1, 2)},
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
    finally:
        sys.path.remove(str(tmp_path))
        _drop_generated_modules(generated_package)

    transform_source = files[f"{generated_package}/pyspark/transforms/order.py"]
    runtime_source = files[f"{generated_package}/runtime/schema_assert.py"]
    assert "udf(" not in transform_source
    assert ".rdd" not in transform_source
    assert "toPandas" not in transform_source
    assert "collect(" not in transform_source
    assert "collect(" not in runtime_source


def test_multiple_schema_parameters_and_results_match_online_and_generated(spark, tmp_path) -> None:
    generated_package = "integration_multi_generated"
    files = PySpark.render.project()(
        PySpark.plan.lower()(compile_transform(AddLookupProduct)),
        source_transform=f"{__name__}.AddLookupProduct",
        generated_package=generated_package,
        source_schema_modules={
            __name__: [LookupOrder, LookupProduct, LookupEnriched],
        },
    )
    _write_files(tmp_path, files)

    sys.path.insert(0, str(tmp_path))
    try:
        importlib.invalidate_caches()
        schemas = importlib.import_module(f"{generated_package}.pyspark.schemas.test_v1_backend_matrix")
        frames = {
            "orders": spark.createDataFrame(
                [("o-1", "p-1"), ("o-2", "missing")],
                schema=schemas.LOOKUP_ORDER_SCHEMA,
            ),
            "products": spark.createDataFrame(
                [("p-1", "Engine")],
                schema=schemas.LOOKUP_PRODUCT_SCHEMA,
            ),
        }

        online = AddLookupProduct(**frames).run(StructureSession(spark=spark, execution_mode="online"))
        generated = AddLookupProduct(**frames).run(
            StructureSession(
                spark=spark,
                execution_mode="generated",
                generated_package=generated_package,
            )
        )

        for name in ("accepted", "audited"):
            online_rows = [row.asDict() for row in online[name].orderBy("id").collect()]
            generated_rows = [row.asDict() for row in generated[name].orderBy("id").collect()]
            assert (
                online_rows
                == generated_rows
                == [
                    {"id": "o-1", "product_name": "Engine"},
                    {"id": "o-2", "product_name": None},
                ]
            )
    finally:
        sys.path.remove(str(tmp_path))
        _drop_generated_modules(generated_package)


def _render_generated_project(generated_package: str) -> dict[str, str]:
    return PySpark.render.project()(
        PySpark.plan.lower()(compile_transform(EnrichOrders)),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        generated_package=generated_package,
        source_schema_modules=_source_schema_modules(),
    )


def _source_schema_modules():
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


def _write_files(root: Path, files: dict[str, str]) -> None:
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def _run_generated_orders_transform(spark, generated_package: str, schemas):
    invocation = EnrichOrders(**_input_frames(spark, schemas))
    session = StructureSession(spark=spark, execution_mode="generated", generated_package=generated_package)
    return invocation.run(session)


def _run_online_orders_transform(spark, schemas):
    inputs = _input_frames(spark, schemas)
    invocation = EnrichOrders(**inputs)
    session = StructureSession(spark=spark, execution_mode="online")
    return invocation.run(session)


def _input_frames(spark, schemas) -> dict[str, object]:
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


def _drop_generated_modules(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            sys.modules.pop(name, None)
