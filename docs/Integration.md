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

## Generated Artifact Review

Never hand-edit generated files. If generated output is wrong, fix the source model, configuration, or Structure
compiler.

1. Change Structure source, schemas, or configuration.
2. Run `structure compile`.
3. Review the generated transform, schema, docs, and traceability diffs.
4. Commit source and generated output together.
5. In CI, run `structure compile --fail-on-diff`.

## Packaged Wheel

Package Structure and run it from the same environment that owns the Spark job dependencies.

```bash
poetry build
pip install dist/*.whl
structure check
structure compile
```

For Spark jobs, package the application source and generated directory together. Add both roots to `PYTHONPATH` or the
job's wheel/archive configuration.

## Airflow Orchestration

Airflow should own scheduling, retries, Spark submission, input locations, output locations, and streaming lifecycle.
Structure should own only transform compilation and generated transform execution.

```python
from structure import StructureConfig, StructureSession
from orders.transforms.enrich import EnrichOrders

config = StructureConfig.resolve(project_root="/opt/orders", execution_mode="generated")
session = StructureSession(spark=spark, config=config, ctx={"run_id": run_id})

result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
).run(session)
```

For Structured Streaming jobs, keep source construction, sink configuration, checkpoints, triggers, output modes, query
names, `start()`, `stop()`, and `awaitTermination()` in the Airflow/runner-owned PySpark layer. Pass the streaming
DataFrame into Structure as a caller-supplied input, then apply the sink and lifecycle policy around the returned
DataFrame. The tested recipe shape is [`examples/streams/adoption.py`](../examples/streams/adoption.py).

## Managed Spark Jobs

For Databricks, EMR, Glue, or another managed Spark runtime:

- build or install the application package before the job starts;
- include the generated directory in the submitted artifact;
- set `[tool.structure.plugin.pyspark].profile` to the deployed PySpark range;
- run `structure compile --fail-on-diff` before promotion;
- run live parity tests in the managed runtime before claiming target evidence.

Do not treat local pytest results as vendor certification. Record the exact runtime, PySpark version, command, and
result when publishing release evidence.

## Recovery Links

- Source roots and generated imports: [Configuration](Configuration.md#path-settings)
- Generated transform import failures: [Troubleshooting](Troubleshooting.md#problem-generated-code-execution-generated-transform-is-not-importable)
- PySpark target mismatch: [Troubleshooting](Troubleshooting.md#problem-compatibility-configured-pyspark-target-does-not-support-a-generated-feature)
- Stale generated output: [Troubleshooting](Troubleshooting.md#problem-generated-code-review-generated-output-is-stale)
