from __future__ import annotations

import shutil
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import pytest

from structure import *
from structure.lib.testing import (
    assert_check_success,
    assert_compile_success,
    assert_expected_diagnostic,
    assert_generated_fresh,
    assert_generated_snapshot,
    assert_online_generated_parity,
    generated_files,
)


@contextmanager
def workspace_tmp() -> Iterator[Path]:
    root = (Path(".pytest-workspace-tmp") / uuid4().hex).resolve()
    root.mkdir(parents=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)
        _drop("orders")


def test_pytest_helpers_cover_compiler_freshness_and_snapshots() -> None:
    """As a developer, I can use pytest helpers for compiler checks, freshness, and snapshots."""

    with workspace_tmp() as root:
        _write_project(root)

        checked = assert_check_success(project_root=root)
        compiled = assert_compile_success(project_root=root)
        assert_generated_fresh(project_root=root)

        snapshot = generated_files(root / "generated")
        assert "structure_generated/pyspark/transforms/transforms.py" in snapshot
        assert "docs/index.md" in snapshot
        assert_generated_snapshot(root / "generated", snapshot)

        target = root / "generated" / "structure_generated" / "pyspark" / "transforms" / "transforms.py"
        target.write_text(target.read_text(encoding="utf-8") + "\n# stale edit\n", encoding="utf-8")

        assert "Structure check passed" in checked
        assert "Structure compile passed" in compiled
        with pytest.raises(AssertionError, match="Generated Structure output is stale"):
            assert_generated_fresh(project_root=root)


def test_pytest_helpers_cover_expected_diagnostics() -> None:
    """As a developer, I can use pytest helpers for expected diagnostics."""

    diagnostic = assert_expected_diagnostic(
        lambda: compile_transform(BadTotal),
        "SCHEMA-E0301",
        problem_contains="may produce null",
        use_contains="coalesce",
        source_endswith="BadTotal.normalize",
    )

    assert isinstance(diagnostic.source, str)


def test_pytest_helpers_cover_online_generated_parity() -> None:
    """As a developer, I can use pytest helpers for execution/generated-code parity."""

    online = TransformResultLike(
        {
            "published": FrameLike(
                columns=("id", "total"),
                rows=[{"id": "b", "total": "2.00"}, {"id": "a", "total": "1.00"}],
                schema="struct<id:string,total:decimal(12,2)>",
            )
        }
    )
    generated = {
        "published": FrameLike(
            columns=("id", "total"),
            rows=[{"id": "a", "total": "1.00"}, {"id": "b", "total": "2.00"}],
            schema="struct<id:string,total:decimal(12,2)>",
        )
    }

    assert_online_generated_parity(lambda: online, lambda: generated)


def _write_project(root: Path) -> None:
    _drop("orders")
    package = root / "src" / "orders"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "schemas.py").write_text(
        "\n".join(
            [
                "from structure import Decimal, String, Schema, field",
                "",
                "class OrderRaw(Schema):",
                "    id = field(String(), nullable=False)",
                "    total = field(String(), nullable=True)",
                "",
                "class OrderNormalized(Schema):",
                "    id = field(String(), nullable=False)",
                "    total = field(Decimal(12, 2), nullable=False)",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (package / "transforms.py").write_text(
        "\n".join(
            [
                "from structure import Transform, coalesce, input, output, to_decimal, transform, where",
                "from orders.schemas import OrderNormalized, OrderRaw",
                "",
                "@transform",
                "class NormalizeOrders(Transform):",
                "    orders = input(OrderRaw)",
                "    normalized = output(OrderNormalized)",
                "",
                "    def normalize(self, order: OrderRaw) -> OrderNormalized:",
                "        where(order.id.is_not_null())",
                "        return OrderNormalized(",
                "            id=order.id,",
                "            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),",
                "        )",
                "",
            ]
        ),
        encoding="utf-8",
    )


class Raw(Schema):
    id = field(String(), nullable=False)
    total = field(String(), nullable=True)


class Published(Schema):
    id = field(String(), nullable=False)
    total = field(Decimal(12, 2), nullable=False)


@transform
class BadTotal(Transform):
    rows = input(Raw)
    published = output(Published)

    def normalize(self, row: Raw) -> Published:
        return Published(id=row.id, total=to_decimal(row.total, precision=12, scale=2))


class TransformResultLike:

    def __init__(self, values: dict[str, object]) -> None:
        self._values = values

    def as_dict(self) -> dict[str, object]:
        return dict(self._values)


class FrameLike:

    def __init__(self, *, columns: tuple[str, ...], rows: list[dict[str, object]], schema: str) -> None:
        self.columns = columns
        self._rows = rows
        self.schema = SchemaLike(schema)

    def collect(self) -> list[RowLike]:
        return [RowLike(row) for row in self._rows]


class RowLike:

    def __init__(self, values: dict[str, object]) -> None:
        self._values = values

    def asDict(self, recursive: bool = False) -> dict[str, object]:
        return dict(self._values)


class SchemaLike:

    def __init__(self, text: str) -> None:
        self._text = text

    def simpleString(self) -> str:
        return self._text


def _drop(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            sys.modules.pop(name, None)
