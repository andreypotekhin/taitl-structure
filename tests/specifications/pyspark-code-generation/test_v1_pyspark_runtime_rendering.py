import sys

from structure.app.target.pyspark.api import PySpark


def test_v1_runtime_module_renderer_is_spark_free() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    text = PySpark.render.runtime()()

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert text.startswith("from pyspark.sql import functions as F\n")


def test_v1_runtime_module_renderer_contains_schema_helpers_and_hook_inputs() -> None:
    text = PySpark.render.runtime()()

    assert "def assert_schema(df, schema, *, name: str, mode: str) -> None:" in text
    assert 'raise ValueError(f"{name} is missing required column(s): {names}")' in text
    assert 'if mode == "strict":' in text
    assert "def project_schema(df, schema):" in text
    assert "return df.select(*(F.col(field.name).cast(field.dataType).alias(field.name) for field in schema))" in text
    assert "class HookInputs:" in text
    assert 'raise AttributeError("HookInputs is read-only")' in text
    assert "class ResultSchemas(Mapping):" in text
    assert "object.__setattr__(self, 'schema', ResultSchemas(schema))" in text
    assert 'raise AttributeError("ResultSchemas is read-only")' in text
