import sys
from importlib.resources import files

from structure.plugin.pyspark import PySpark
from structure.plugin.pyspark.render.commands.RenderPySparkRuntimeModule import (
    RESOURCE_PACKAGE,
    RUNTIME_MODULE_RESOURCE,
)


def test_v1_runtime_module_renderer_is_spark_free() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    text = PySpark.render.runtime()()

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert "from pyspark.sql import functions as F\n" in text


def test_v1_runtime_module_renderer_uses_packaged_resource() -> None:
    text = PySpark.render.runtime()()
    resource = files(RESOURCE_PACKAGE).joinpath(RUNTIME_MODULE_RESOURCE).read_text(encoding="utf-8")

    assert text == resource
    compile(text, RUNTIME_MODULE_RESOURCE, "exec")


def test_v1_runtime_module_renderer_contains_schema_helpers() -> None:
    text = PySpark.render.runtime()()

    assert "def assert_schema(df, schema, *, name: str, mode: str) -> None:" in text
    assert 'raise ValueError(f"{name} is missing required column(s): {names}")' in text
    assert "def _same_data_type(actual, expected) -> bool:" in text
    assert 'if mode == "strict":' in text
    assert "def project_schema(df, schema):" in text
    assert "return df.select(*(F.col(field.name) for field in schema))" in text
    assert "HookInputs" not in text
    assert "class ResultSchemas(Mapping):" in text
    assert "object.__setattr__(self, 'schema', ResultSchemas(schema, aliases=output_aliases))" in text
    assert 'raise AttributeError("ResultSchemas is read-only")' in text
    assert "def _alias_index(aliases):" in text
