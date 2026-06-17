# Design: PySpark Code Generator

## Purpose

The PySpark generator lowers Structure IR to deterministic, readable PySpark modules.

## Generated Artifacts

```text
generated/structure_generated/
  pipeline_src/pyspark/
    schemas/
    transforms/
  runtime/
  lineage/
```

## Transform Class Generation

Each source transform class maps to one generated class.

```python
class EnrichOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()  # only if hooks exist

    def run(self, *, orders: DataFrame, customers: DataFrame) -> DataFrame:
        ...
```

## Codegen Rules

- Generate imports deterministically.
- Use `df` as the current DataFrame variable.
- Use stable aliases derived from schema or step names.
- Prefer `select(...)` for projection.
- Generate `where(...)` before projection when possible.
- Generate joins explicitly.
- Generate schema validation after subtransforms by default.
- Omit hook imports when no hooks exist.
- Format generated code if configured.

## PySpark Evolution Strategy

PySpark API usage belongs in this component, not in symbolic execution or checks.

Use a backend capability registry:

```text
PySparkCapabilities
  version_range
  supports_timestamp_ntz
  supports_testing_utils
  supports_specific_hint
```

The emitter chooses generated syntax based on target capabilities.

## Performance Commitment

Generated compiled paths must not introduce:

- Python UDFs
- pandas UDFs
- RDD operations
- `collect`
- `toPandas`
- row-wise maps

## Compile-Time Performance

Generate files in memory first. Write only changed files. Format only changed files. Support parallel generation by transform module.
