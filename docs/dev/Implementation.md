# Implementation

## Implementation Phases

### Phase 1: Minimal Vertical Slice

- Schema declarations.
- `input(...)`.
- `@transform` discovery.
- Single schema-returning subtransform.
- Symbolic field references.
- Projection generation.
- Spark `StructType` generation.
- Generated transform class.
- Input and output schema validation.
- CLI `structure check`.
- CLI `structure compile`.
- Seed config generation.

### Phase 2: v1 Complete

- Source-order multi-subtransform chains.
- Intermediate schema validation.
- `where(...)` filtering.
- `@expr_fn` helpers.
- `@after(method)` and `@before(method)` hooks.
- Hook signature validation.
- `join_one(...)`.
- Serial N-step joins.
- Clean no-hook generated code.
- Structured compiler errors.
- Config workaround suggestions in errors.
- Streaming compatibility checks.
- Basic LDJSON lineage.
- TOML configuration.

### Phase 3: v2

- Aggregations.
- Windowing.
- Deduplication helpers.
- Higher-order functions for arrays and maps.
- Caching and persistence hints.
- Join strategy controls.
- Advanced aggregation and grouping.
- Optional field-level lineage.
- Aggregation and window validation rules.

### Phase 4: v3

- Generated streaming reads.
- Generated streaming writes.
- Watermarks.
- Triggers.
- Checkpoints.
- Streaming lifecycle configuration.
- Advanced stateful validation.

## Configuration

Structure supports convention-first behavior and optional TOML.

Config resolution order:

1. explicit CLI flags
2. `pyproject.toml` `[tool.structure]`
3. `structure.toml`
4. seed defaults

Minimal model:

```python
@dataclass
class StructureConfig:
    source_dir: str = "structure/src"
    generated_dir: str = "structure/generated"
    target_backend: str = "pyspark"
    target_pyspark: str | None = ">=3.5,<4.2"

    validate_inputs: bool = True
    validate_intermediate: bool = True
    validate_outputs: bool = True

    lineage: LineageLevel = LineageLevel.BASIC
    streaming_compatibility_checks: bool = True
    strict_performance: bool = True

    format_generated: bool = True
    fail_on_diff: bool = False
```

## Source Discovery

Discovery should:

1. load configured source directory
2. import transform modules
3. collect classes marked with `@transform`
4. preserve class member definition order
5. identify inputs
6. identify public schema-returning methods
7. identify expression helpers
8. identify hooks

A public method becomes a subtransform when:

- it is an instance method
- it does not start with `_`
- it has a return annotation that is a `Schema`
- it is not a hook
- it is not an expression helper

Ambiguous public methods should fail compilation.

## Symbolic Execution

Symbolic execution runs each subtransform with schema proxy objects.

Example:

```python
result = impl.normalize(symbolic_order)
```

During execution:

```python
order.customer_id
```

returns a `FieldRef`.

```python
lower(trim(order.customer_id))
```

returns nested call expressions.

```python
where(order.id.is_not_null())
```

records a filter predicate in the active symbolic context.

Subtransform output object construction becomes a projection IR node.

## Unsupported Operations

Unsupported operations should produce structured errors.

Error fields:

- error code
- transform class
- subtransform method
- output field, when known
- source expression, when available
- problem
- why it matters
- inline DSL alternative
- `@expr_fn` helper alternative
- hook alternative
- config workaround, if one exists

Example:

```text
CompileError: Unsupported expression in compiled subtransform

Transform:
  EnrichOrders

Subtransform:
  normalize

Output field:
  OrderNormalized.customer_id

Source expression:
  order.customer_id.strip().lower()

Problem:
  Python string methods .strip() and .lower() cannot be compiled to Spark Column expressions.

Why this matters:
  Compiled subtransforms must lower to Spark-plan-visible Column expressions.
  Silent fallback to UDFs would reduce optimizer visibility and add Python serialization overhead.

Use direct DSL functions:
  customer_id=lower(trim(order.customer_id))

For reuse:
  @expr_fn
  def clean_id(value):
      return lower(trim(value))

For arbitrary PySpark:
  @after(normalize)
  def clean_id_column(self, *, df, spark, ctx):
      return df.withColumn("customer_id", F.lower(F.trim(F.col("customer_id"))))

Config workaround:
  No configuration setting allows unsupported Python in compiled subtransforms.
```

## Generated Code

Generated code should be deterministic.

Rules:

- one generated class per transform class
- stable `run(...)` method
- stable `df`, `spark`, and `ctx` names
- hook imports only when hooks exist
- source transform instantiation only when hooks exist
- section comments for each subtransform
- validation after each subtransform by default
- final validation always on unless explicitly disabled by specialized mode
- no UDFs from compiled subtransforms
- no `collect`, `toPandas`, or `rdd` in compiled generated paths

## v2 Manual Optimization Features

Structure should support manual optimization features without compromising the performance contract.

Planned v2 features:

- Higher-order expression helpers for arrays and maps.
- Cache and persist hints at subtransform boundaries.
- Join strategy controls such as broadcast, shuffle hash, sort-merge, and repartition hints where appropriate.
- Advanced aggregation and grouping APIs.
- Skew handling hints where they can be emitted safely.

These features should compile to explicit Spark DataFrame operations or Spark hints. They should not create opaque Python execution paths.

Example future cache hint:

```python
@cache_after(normalize, storage=StorageLevel.MEMORY_AND_DISK)
def normalize(...):
    ...
```

Alternative future syntax:

```python
@optimize(cache=Cache.MEMORY_AND_DISK)
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

The exact API should be finalized in v2.

## LDJSON Lineage

Emit one LDJSON file per transform.

Default basic events:

```json
{"type":"transform","name":"EnrichOrders","generated_class":"EnrichOrdersGenerated"}
{"type":"input","transform":"EnrichOrders","name":"orders","schema":"OrderRaw"}
{"type":"step","transform":"EnrichOrders","name":"normalize","input_schema":"OrderRaw","output_schema":"OrderNormalized"}
{"type":"join","transform":"EnrichOrders","step":"add_customer","input":"customers","schema":"Customer","join_type":"left","cardinality":"one","hint":"broadcast"}
{"type":"hook","transform":"EnrichOrders","step":"normalize","stage":"after","method":"remove_negative_totals","visibility":"opaque"}
{"type":"output","transform":"EnrichOrders","schema":"OrderEnriched"}
```

Field-level events are optional and should be disabled by default to avoid bloat.
