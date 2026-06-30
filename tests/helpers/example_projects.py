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
        from orders.schemas import OrderPublished, OrderRaw
        from orders.transforms import PublishOrders

        return PySpark.render.project()(
            PySpark.plan.lower()(compile_transform(PublishOrders)),
            source_transform="orders.transforms.PublishOrders",
            generated_package="structure_generated.orders",
            source_schema_modules={"orders.schemas": [OrderRaw, OrderPublished]},
        )


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
