from __future__ import annotations


class RenderPySparkRuntimeModule:

    def __call__(self) -> str:
        return "\n".join(
            [
                "from pyspark.sql import functions as F",
                "",
                "",
                'def assert_schema(df, schema, *, name: str, mode: str) -> None:',
                "    actual = df.schema",
                "    actual_names = set(actual.fieldNames())",
                "    expected_names = {field.name for field in schema}",
                "    missing = expected_names - actual_names",
                "    if missing:",
                '        names = ", ".join(sorted(missing))',
                '        raise ValueError(f"{name} is missing required column(s): {names}")',
                '    if mode == "strict":',
                "        extra = actual_names - expected_names",
                "        if extra:",
                '            names = ", ".join(sorted(extra))',
                '            raise ValueError(f"{name} has unexpected column(s): {names}")',
                "    for expected in schema:",
                "        actual_field = actual[expected.name]",
                "        if actual_field.dataType != expected.dataType:",
                '            raise ValueError(f"{name}.{expected.name} expected {expected.dataType}, got {actual_field.dataType}")',
                "",
                "",
                "def project_schema(df, schema):",
                "    return df.select(*(F.col(field.name) for field in schema))",
                "",
                "",
                "class HookInputs:",
                "",
                "    def __init__(self, **inputs):",
                '        object.__setattr__(self, "_structure_frozen", False)',
                '        object.__setattr__(self, "_structure_names", tuple(inputs))',
                "        for name, value in inputs.items():",
                "            object.__setattr__(self, name, value)",
                '        object.__setattr__(self, "_structure_frozen", True)',
                "",
                "    def __setattr__(self, name, value):",
                '        if getattr(self, "_structure_frozen", False):',
                '            raise AttributeError("HookInputs is read-only")',
                "        object.__setattr__(self, name, value)",
                "",
            ]
        )


render_pyspark_runtime_module = RenderPySparkRuntimeModule()
