# Spark Connect

This specification defines Spark Connect support for Structure. Spark Connect is a PySpark target variant selected by
configuration, not a separate backend and not a change to the public transform authoring model.

## Configuration

Spark Connect uses the existing PySpark backend with a variant:

```toml
[tool.structure]
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "spark-connect"
```

`target_variant = "ordinary"` remains the default. A project must opt in to Spark Connect explicitly.

Compiler commands must resolve this configuration from static metadata only. They must not import PySpark, start Java,
create a `SparkSession`, connect to a Spark server, or inspect the installed Spark runtime.

## Support Level

Sprint 09 promotes Spark Connect from experimental to supported for completed compiler-visible batch features when the
acceptance checks in this specification pass. The support claim is limited to:

- execution through `StructureSession`;
- generated PySpark execution;
- compiler-visible Structure DSL features completed in v1 and v2;
- schema-only validation and strict projection;
- capability diagnostics for unsupported ordinary-only behavior.

The support claim does not cover:

- Structure-owned streaming source, sink, trigger, checkpoint, watermark, output mode, or state policy generation;
- storage write orchestration;
- arbitrary hook internals;
- direct use of classic-only PySpark internals;
- hidden fallback to SQL string rendering, local materialization, row-wise execution, or undeclared Python UDFs.

## Required Capabilities

The Spark Connect capability profile must support the completed batch capability families that ordinary PySpark
supports when those families lower through public DataFrame and Column APIs:

```text
backend.spark_connect_dataframe
expression.field_ref
expression.literal
expression.projection
expression.filter
expression.boolean_ops
expression.equality
expression.null_safe_equality
expression.cast
expression.standard_helper_call
join.lookup_join
join.exists
join.not_exists
join.inner_join
join.lookup_dedupe
join.temporal_one
join.as_of_one
join.left_join
join.inner_join
join.left_semi_join
join.left_anti_join
join.composite_equi_join
join.rowset_join
join.right_join
join.full_join
join.cross_join
join.non_equi_condition
join.disjunctive_condition
aggregate.group_by
pyspark.ordered_timeline_scan
aggregate.rollup
aggregate.cube
aggregate.count
aggregate.count_distinct
aggregate.sum
aggregate.min
aggregate.max
aggregate.avg
aggregate.approx_count_distinct
aggregate.approx_percentile
aggregate.bool_and
aggregate.bool_or
aggregate.collect_list
aggregate.collect_set
aggregate.corr
aggregate.covar
aggregate.filtered_metric
aggregate.first_value
aggregate.grouping_id
aggregate.is_grouped
aggregate.last_value
aggregate.stddev
aggregate.variance
higher_order.array_aggregate
higher_order.array_distinct
higher_order.array_exists
higher_order.array_transform
higher_order.array_filter
higher_order.array_flatten
higher_order.array_forall
higher_order.array_position
higher_order.array_sort_by
higher_order.array_zip_with
higher_order.map_entries
higher_order.map_transform_values
higher_order.map_filter
higher_order.map_from_entries
higher_order.map_keys
higher_order.map_transform_keys
higher_order.map_values
higher_order.map_zip_with
dedupe.drop_duplicates
window.avg
window.count
window.count_distinct
window.row_number
window.rank
window.dense_rank
window.cume_dist
window.first_value
window.lag
window.last_value
window.lead
window.max
window.min
window.nth_value
window.ntile
window.percent_rank
window.sum
window.rolling_sum
window.rolling_avg
window.rolling_min
window.rolling_max
window.select_latest
window.select_earliest
validation.schema_only_validation
validation.strict_projection
validation.allow_extra_projection
imports.generated_pyspark_imports
```

The support claim also includes the implemented public shapes behind these capabilities: `rowset_join(...)`,
`right_join(...)`, `full_join(...)`, `cross_join(..., allow_cartesian=True)`, non-equi and disjunctive predicates,
`rollup(...)`, `cube(...)`, grouping metadata helpers, additional exact/statistical/approximate/collection aggregate
metrics, metric-local filters, reusable explicit windows, distribution/value/window aggregate helpers, and the expanded
array/map higher-order helper set.

Deferred capability boundaries remain explicit. `join.using_keys`, broad join strategy capability names,
`window.window_project`, `optimization.repartition`, generated documentation, and production incremental compile stay
unsupported until their owning specifications admit them.

Optimization capabilities must be admitted individually. `optimization.cache` is part of the current common PySpark
profile. `optimization.persist`, `optimization.repartition`, `optimization.coalesce`, `optimization.checkpoint`, and
join strategy hints are supported for Spark Connect only when the implementation uses public Connect-compatible
DataFrame APIs and live tests prove that the directive does not change row or schema semantics.

## Ordinary-Only Requirements

The Spark Connect variant must reject these requirements before execution or generation:

```text
backend.spark_context
backend.rdd_access
backend.jvm_access
backend.py4j_access
backend.private_classic_fields
runtime.local_spark_context
runtime.driver_only_jvm_gateway
```

Diagnostics must include the configured target variant, the source location when available, a short explanation, and a
link to this specification or the public Spark Connect reference.

## Runtime Contract

`StructureSession` remains:

```python
StructureSession(spark=spark, ctx=ctx, config=config)
```

For Spark Connect, `spark` is a caller-created Spark Connect session. Structure must not create or own the connection.
Execution and generated-code execution must call the same public DataFrame and Column methods used by the shared
PySpark recipe layer.

Runtime code must not access:

- `spark.sparkContext`;
- `dataframe.rdd`;
- `dataframe._jdf`;
- `spark._jvm`;
- Py4J gateway objects;
- private fields whose availability depends on classic in-process PySpark.

## Generated Code Contract

Generated modules for Spark Connect use the same package layout, class names, constructor shape, and `run(...)`
signature as ordinary generated PySpark. The header metadata must include the configured variant.

Generated code must remain readable and reviewable. It must not branch on Spark Connect at runtime unless the public
PySpark API requires a variant-specific call. It must not emit classic-only internals or hidden fallback code.

Generated-code snapshots for representative v1/v2 fixtures must prove:

- imports remain ordinary public PySpark imports;
- no `_jdf`, `sparkContext`, `.rdd`, `_jvm`, or Py4J gateway access appears;
- supported operations render through shared PySpark recipes;
- unsupported operations fail before rendering.

## Hook Contract

Hooks are supported on Spark Connect only as opaque user-owned PySpark code. Structure validates the hook signature and
target scope; it does not certify arbitrary hook body internals.

Rules:

- a hook with `target_backend = "pyspark"` applies to both ordinary and Spark Connect unless a future variant-specific
  hook target is added;
- a hook that declares an ordinary-only requirement is rejected for Spark Connect;
- uninspectable hook internals may produce warnings in strict compatibility reports;
- runtime hook failures caused by classic-only internals are wrapped with `CONNECT-E2601` when Structure can detect the
  failure class or message reliably.

The recommended fix for a Connect-incompatible hook is to move the logic into compiler-visible Structure DSL. If the
logic must remain opaque, the user should rewrite it using public Connect-compatible PySpark APIs or scope it away from
Spark Connect.

## StructureTools Contract

StructureTools schema generation may accept a Spark Connect session for metadata operations that Spark Connect supports,
such as reading a DataFrame-like `.schema` or `spark.table(...).schema`. It must not require SparkContext or JVM access.

Path-based schema extraction with `spark.read` is supported only when the Connect session supports the requested reader
format and options. Unsupported reader behavior must fail with a clear `StructureToolError` that names Spark Connect,
preserves the original cause, and suggests passing an explicit schema object when live metadata access is unavailable.

## Diagnostics

Spark Connect diagnostics use the backend diagnostic family unless a narrower component already owns the failure.

Required diagnostic cases:

- unsupported target variant value;
- unsupported backend capability for `target_variant = "spark-connect"`;
- classic-only internal access detected statically;
- live Connect session unavailable during explicit runtime verification;
- Connect server/version mismatch in `structure doctor` or the manual verification script;
- unsupported hook or generated runtime boundary detected as `CONNECT-E2601`;
- unsupported StructureTools live metadata path.

Every diagnostic must include a user action: change `target_variant` to `ordinary`, remove the classic-only API, rewrite
the operation in compiler-visible DSL, pass a Connect-compatible session, or use an explicit schema.

## Verification Matrix

The minimum support matrix is:

```text
ordinary PySpark, PySpark 3.5.x, online
ordinary PySpark, PySpark 3.5.x, generated
ordinary PySpark, PySpark 4.0.x, online
ordinary PySpark, PySpark 4.0.x, generated
Spark Connect, PySpark 3.5.x, online
Spark Connect, PySpark 3.5.x, generated
Spark Connect, PySpark 4.0.x, online
Spark Connect, PySpark 4.0.x, generated
```

If CI cannot run the full Connect matrix in the current environment, Sprint 09 must add a documented manual verification
script and mark CI coverage as a release blocker before publishing a stable support claim.

## Acceptance Criteria

Spark Connect batch support is complete when:

- `target_variant = "spark-connect"` resolves a supported backend capability profile;
- completed v1/v2 batch fixtures pass execution/generated-code parity against a real Spark Connect session;
- compiler commands remain Spark-free;
- generated source contains no classic-only internals;
- unsupported ordinary-only features fail before runtime or generation;
- public docs state the supported batch scope and remaining exclusions;
- Sprint 09 records the evidence and any gaps.
