from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
from click.testing import CliRunner

from structure import Schema, Transform, input, output
from structure.core.cli.api import cli
from structure.core.cli.commands.DiscoverStructureProject import DiscoverStructureProject
from structure.core.compiler.api import Compiler
from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.core.compiler.frontend.commands.CompileTransform import CompileTransform
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.plugin.pyspark import field


@dataclass(frozen=True)
class FakeType:
    name: str
    args: tuple = ()


class FakeTypes:

    @staticmethod
    def StructType(fields):
        return FakeType("StructType", tuple(fields))

    @staticmethod
    def StructField(name, data_type, nullable):
        return FakeType("StructField", (name, data_type, nullable))

    @staticmethod
    def StringType():
        return FakeType("StringType")


def test_colocated_intermediate_schema_compiles_and_generates(tmp_path: Path, monkeypatch) -> None:
    _write_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    checked = CliRunner().invoke(cli, ["check"])
    compiled = CliRunner().invoke(cli, ["compile"])
    unchanged = CliRunner().invoke(cli, ["compile", "--fail-on-diff"])

    project = DiscoverStructureProject()(StructureConfig.resolve(project_root=tmp_path))
    transform = project.transforms[0]
    artifact = transform.compile(project_root=tmp_path, schema_types=FakeTypes)

    assert checked.exit_code == 0, checked.output
    assert "schemas: 3" in checked.output
    assert compiled.exit_code == 0, compiled.output
    assert unchanged.exit_code == 0, unchanged.output
    assert [schema.__name__ for schema in project.schema_modules["orders.transforms.publish"]] == ["OrderNormalized"]
    assert artifact.schemas.steps["normalize"].name == "StructType"
    assert Path("generated/structure_generated/pyspark/schemas/publish.py").exists()
    assert Path("generated/structure_generated/pyspark/transforms/publish.py").exists()
    assert Path("generated/docs/schemas/OrderNormalized.md").exists()
    assert Path("generated/docs/transforms/orders.transforms.publish.PublishOrders.json").exists()
    generated = Path("generated/structure_generated/pyspark/transforms/publish.py").read_text(encoding="utf-8")
    assert "from structure_generated.pyspark.schemas.publish import ORDER_NORMALIZED_SCHEMA" in generated


def test_nested_schema_fails_before_annotation_resolution() -> None:
    class Nested(Transform):
        class Normalized(Schema):
            id = field.string(nullable=False)

        rows = input(Normalized)
        normalized = output(Normalized)

        def normalize(self, row: Normalized) -> Normalized:
            return row

    _require_nested_schema_error(Nested, "Nested.Normalized")


def test_nested_schema_inherited_from_transform_base_fails() -> None:
    class Base(Transform):
        class Normalized(Schema):
            id = field.string(nullable=False)

    class Nested(Base):
        rows = input(Base.Normalized)
        normalized = output(Base.Normalized)

        def normalize(self, row: Base.Normalized) -> Base.Normalized:
            return Base.Normalized(id=row.id)

    _require_nested_schema_error(Nested, "Base.Normalized")


def test_module_level_schema_assigned_to_transform_attribute_is_allowed() -> None:
    class Row(Schema):
        id = field.string(nullable=False)

    class Copy(Transform):
        schema = Row
        rows = input(Row)
        copied = output(Row)

        def copy(self, row: Row) -> Row:
            return Row(id=row.id)

    assert not CompileTransform._nested_schema(Row, Copy)


def _require_nested_schema_error(transform: type[Transform], name: str) -> None:
    with pytest.raises(StructureCompileError) as raised:
        Compiler.frontend.compile()(transform, materialize_schemas=False)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0402"
    assert diagnostic.problem == f"{name} is a Schema declared inside a Transform."
    assert "Move Normalized to module scope" in diagnostic.use


def _write_project(root: Path) -> None:
    package = root / "src" / "orders"
    schemas = package / "schemas"
    transforms = package / "transforms"
    schemas.mkdir(parents=True)
    transforms.mkdir(parents=True)
    for directory in (package, schemas, transforms):
        (directory / "__init__.py").write_text("", encoding="utf-8")
    (schemas / "order.py").write_text(
        """
from structure import Schema
from structure.plugin.pyspark import field


class OrderRaw(Schema):
    id = field.string(nullable=False)


class OrderPublished(Schema):
    id = field.string(nullable=False)
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (transforms / "publish.py").write_text(
        """
from structure import Schema, Transform, input, output
from structure.plugin.pyspark import field
from orders.schemas.order import OrderPublished, OrderRaw


class OrderNormalized(Schema):
    id = field.string(nullable=False)


class PublishOrders(Transform):
    orders = input(OrderRaw)
    published = output(OrderPublished)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized(id=order.id)

    def publish(self, order: OrderNormalized) -> OrderPublished:
        return OrderPublished(id=order.id)
""".strip()
        + "\n",
        encoding="utf-8",
    )
    _drop_orders_modules()


def _drop_orders_modules() -> None:
    for name in tuple(sys.modules):
        if name == "orders" or name.startswith("orders."):
            sys.modules.pop(name)
