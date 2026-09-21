import ast
from typing import cast

from structure import Schema, Transform, input, output, special
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import PySpark, array, string, struct, types
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class Detail(Schema):
    label = string(nullable=False)


class NestedRow(Schema):
    details = array(struct(Detail), contains_null=False, nullable=False)


class Base(Schema):
    id = string(nullable=False)


class Derived(Base):
    label = string(nullable=False)


class CopyNested(Transform):
    rows = input(NestedRow)
    result = output(NestedRow)

    def copy(self, row: NestedRow) -> NestedRow:
        return row


class ExtractLabel(Transform):
    rows = input(Base)
    result = output(Derived)

    def label(self, row: Base) -> Derived:
        detail = self.detail(row.id)
        return Derived.base(row)(label=detail.label)

    @special(type="udf", return_type=types.struct(Detail), nullable=False)
    def detail(value):
        return {"label": value}


def test_empty_schema_discovery_includes_nested_and_inherited_dependencies():
    plans: dict[str, PySparkExecutionPlan] = {
        f"{cls.__module__}.{cls.__name__}": cast(
            PySparkExecutionPlan,
            Compiler.frontend.compile()(cls, materialize_schemas=False).lowered,
        )
        for cls in (CopyNested, ExtractLabel)
    }
    files = PySpark.render.project().source_unit(
        plans, source_module=__name__, source_schema_modules={}, generated_package="complete_schemas"
    )
    source = "\n".join(files.values())
    for name in ("DETAIL", "NESTED_ROW", "BASE", "DERIVED"):
        assert f"{name}_SCHEMA = " in source
    schema = next(text for path, text in files.items() if "/schemas/" in path and "DETAIL_SCHEMA = " in text)
    assert schema.index("DETAIL_SCHEMA = ") < schema.index("NESTED_ROW_SCHEMA = ")
    assert schema.index("BASE_SCHEMA = ") < schema.index("DERIVED_SCHEMA = ")
    for path, text in files.items():
        if path.endswith(".py"):
            ast.parse(text)
    repeated = PySpark.render.project().source_unit(
        plans, source_module=__name__, source_schema_modules={}, generated_package="complete_schemas"
    )
    assert repeated == files
