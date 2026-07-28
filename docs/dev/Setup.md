# Operational Integration

## Local Development

Install the project dependencies, keep Structure source and generated output importable, and run compiler checks before
reviewing generated diffs.

```bash
poetry install
poetry run structure check
poetry run structure compile
```

Example project layout:

```text
src/
generated/
pyproject.toml
```

Configure:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"

[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

Use `execution_mode = "online"` while developing when you want Structure to compile in memory and execute immediately.
Use `execution_mode = "generated"` when you want the checked-in generated PySpark module to run.

## CI

Run a no-Spark compiler lane and treat generated diffs as reviewable source changes.

```bash
poetry install
poetry run structure check
poetry run structure compile --fail-on-diff
poetry run pytest
```

`structure check` and `structure compile` must not require a Spark session. Put live Spark tests in a separate CI lane
so a Spark outage or image problem does not hide compiler regressions.

## Next

[Development](Development.md)

## Troubleshooting

Troubleshooting: [Troubleshooting](Troubleshooting.md)
