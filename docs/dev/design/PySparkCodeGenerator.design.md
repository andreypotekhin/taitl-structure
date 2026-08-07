# Design: PySpark Code Generator

## Purpose

The PySpark generator lowers Structure IR to deterministic, readable PySpark modules. It is the source-text consumer of
the same shared PySpark execution recipes used by the online PySpark runner.

## Generated Artifacts

```text
generated/structure_generated/
  orders/pyspark/
    schemas/
    transforms/
  runtime/
  traceability/  # compiler metadata, not runtime telemetry
```

Generated artifacts are optional for ordinary execution. They remain first-class for provenance, review,
snapshot tests, and projects configured with `execution_mode = "generated"`.

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

With `generated_code_options = ["mirror_methods"]`, the generated constructor accepts original input DataFrames and
`run()` is zero-argument. Compiler-visible schema steps become source-named state transitions over generated instance
fields. Expression specials are inline by default; `embed_exprs`, `embed_hooks`, and `embed_udfs` independently control
expression-helper, raw-hook, and UDF rendering. These choices affect source artifact identity but never alter the
flattened execution plan.

## Codegen Rules

- Generate imports deterministically.
- Use `df` as the current DataFrame variable.
- Use stable aliases derived from schema or step names.
- Prefer `select(...)` for projection.
- Generate `where(...)` before projection when possible.
- Generate joins explicitly.
- Generate schema validation after step methods by default.
- Omit hook imports when no hooks exist.
- Generate a read-only `HookInputs` namespace only when a hook declares `pass_inputs=True`.
- Format generated code if configured.

## Ownership Rules

Generated PySpark is optional committed build output owned by the compiler. The generator must make files stable enough
for code review and snapshot tests, and it must never depend on manual edits inside `generated/`.

If generated output needs to change, developers change Structure source, configuration, or generator code, then
regenerate. CI should run `structure compile --fail-on-diff` to catch stale or manually edited generated files for
projects that commit generated artifacts.

## Online Parity

Generated code and execution must preserve the same transform semantics. Text concerns such as imports and
formatting belong here. Semantic concerns such as expression lowering, join aliasing, validation placement, hook order,
and projection shape belong to the shared contract in [ExecutionSemanticContract.spec.md](../specifications/ExecutionSemanticContract.spec.md).

The generator renders `PySparkExecutionPlan` recipes, or the local implementation equivalent. It must not independently
choose aliases, validation placement, literal typing, hook order, or projection shape while rendering source text.

## PySpark Evolution Strategy

PySpark API usage belongs in the PySpark target layer, not in symbolic execution or generic checks. The generator and
online runner must use the backend capability interface in [BackendCapabilities.spec.md](../specifications/BackendCapabilities.spec.md) when selecting
target-specific syntax or rejecting unsupported features.

## Performance Commitment

Compiled paths must not introduce:

- Python UDFs
- pandas UDFs
- RDD operations
- `collect`
- `toPandas`
- row-wise maps

## Compile-Time Performance

Generate files in memory first. Write only changed files. Format only changed files. Support parallel generation by
transform module.
