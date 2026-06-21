import sys

from structure.app.backend.pyspark.api import render_pyspark_runtime_module


def test_v1_runtime_module_renderer_is_spark_free() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    text = render_pyspark_runtime_module()

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert text.startswith("from pyspark.sql import functions as F\n")


def test_v1_runtime_module_renderer_contains_schema_helpers_and_hook_inputs() -> None:
    text = render_pyspark_runtime_module()

    assert "def assert_schema(df, schema, *, name: str, mode: str) -> None:" in text
    assert 'raise ValueError(f"{name} is missing required column(s): {names}")' in text
    assert 'if mode == "strict":' in text
    assert "def project_schema(df, schema):" in text
    assert "return df.select(*(F.col(field.name) for field in schema))" in text
    assert "class HookInputs:" in text
    assert 'raise AttributeError("HookInputs is read-only")' in text
