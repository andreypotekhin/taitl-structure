# Diagnostics

Below is an index for published diagnostic codes. For the full diagnostic contract, see
[Diagnostics.md](background/Diagnostics.back.md).

## Active Codes

| Code | Severity | Title | Use |
| --- | --- | --- | --- |
| CONF-E0101 | error | Unknown configuration key | Remove the key or correct its spelling. |
| CONF-E0102 | error | Invalid configuration value | Set the value to one of the allowed values. |
| DSL-E0401 | error | Unsupported symbolic expression | Rewrite with Structure DSL helpers or an ordinary compiler-visible helper; use `@special(type="expr")` for explicit metadata, `@special(type="udf")` for intentional scalar Python execution, or a hook for arbitrary DataFrame logic. |
| DSL-E0402 | error | Invalid transform structure | Check decoration, annotations, schema flow, and output fields. |
| DSL-E0404 | error | Ignored compiler code reached | Keep ignored code outside compiled logic, or use a UDF or explicit hook for intentional runtime execution. |
| DSL-E0405 | error | Opaque runtime code reached | Keep opaque runtime code outside compiled logic, or use a UDF or explicit hook for intentional runtime execution. |
| DSL-E0406 | error | Invalid row-level foreach sink | Declare and bind a sink, then target a returned final output row. |
| DSL-W0403 | warning | Python UDF is optimizer-opaque | Prefer a symbolic expression; keep intentional UDFs or set `warn_on_udfs = false`. See the [UDF reference](reference/UDF.ref.md). |
| PYSPARK-W2701 | warning | PySpark query plan is growing through repeated reuse | A branch is reused after joins or unions have made the Spark query plan expensive to build. Add `checkpoint()` or `local_checkpoint()` before reusing it, or restructure around a stable base relation; `cache()` and `persist()` alone do not shorten the query plan. See the [driver-heap gotcha](troubleshooting/memory/spark_driver_heap_oom.gotcha.md). |
| PYSPARK-W2702 | warning | PySpark query plan branch is reused many times | A costly query plan branch feeds several downstream operations. Add a checkpoint at the shared branch or reduce its consumers. |
| PYSPARK-W2703 | warning | Repeated PySpark query plan validation is expensive | Several strict checks are attached to a large query plan. Keep checks at input/output boundaries or checkpoint before validating a reused branch. |
| PYSPARK-W2704 | warning | PySpark output query plan is unusually large | An output depends on many operations without enough planning boundaries. Split the computation with a checkpoint or simplify the output path. |
| PYSPARK-W2705 | warning | Timestamp pattern has no effect on typed temporal input | Remove the pattern, or parse a String expression. |
| PYSPARK-W2706 | warning | Time zone has no effect on NTZ timestamp construction | Remove the zone, or use `make_timestamp_ltz(...)`. |
| PYSPARK-W2707 | warning | Apache and Sedona spatial helpers share Spark SQL registrations | Run each provider in a separate Spark session. Persist WKB and SRID separately for cross-session transfer. See the [geospatial coexistence boundary](reference/Geospatial.ref.md#provider-coexistence-boundary). |
| SCHEMA-E0301 | error | Nullable expression assigned to non-nullable field | Guard the value or provide a non-null default. |
| SCHEMA-E0302 | error | Explicit conversion required | Use an explicit conversion helper such as `to_decimal(...)`. |
| SCHEMA-E0303 | error | Incompatible output field type | Use a compatible expression type or explicit conversion. |
| JOIN-E0601 | error | Unsupported join condition | Use equality pairs with `==` or `null_safe_eq(...)` and combine them with `&`. |
| JOIN-W0601 | warning | lookup_join uniqueness is not proven | Provide deterministic `JoinDedupe`, or use `inner_join(...)` when multiplication is intended. |
| REL-E0701 | error | Relation cardinality assertion failed | Filter or aggregate the relation before asserting `exactly_one(...)`. |
| REL-E0702 | error | Relation uniqueness assertion failed | Deduplicate, aggregate, or correct the source before asserting `require_unique(...)`. |
| REL-E0703 | error | Relation predicate assertion failed | Filter or correct invalid rows before asserting `require_all(...)`. |
| REL-E0704 | error | Relation reference assertion failed | Correct the referenced catalog or filter invalid rows before asserting `require_reference(...)`. |
| REL-E0705 | error | Relation priority selection failed | Add a deterministic priority tie-breaker, fix eligibility, or allow missing candidates. |
| REL-E0706 | error | Relation parent hierarchy assertion failed | Correct missing parents, cycles, depth overruns, or child ordering before asserting `require_parent_hierarchy(...)`. |
| GEN-E0901 | error | Generated output is stale | Run `structure compile` and commit the generated changes. |
| GEN-E0902 | error | Generated transform is not importable | Rebuild generated code or switch to direct execution with `execution_mode = "online"`. |
| GEN-E0903 | error | Embedded hook cannot be generated | Use local imports and a standalone hook body, or remove `embed_hooks`. |
| ONLINE-E1201 | error | Transform input is missing | Pass every declared input DataFrame before `run(session)`. |
| ONLINE-E1202 | error | Direct PySpark runner is not configured | Pass a SparkSession or custom `online_executor`, or switch to generated-code execution with `execution_mode = "generated"`. |
| ONLINE-E1203 | error | Execution mode is unsupported | Use `execution_mode = "online"` or `execution_mode = "generated"`. |
| BACKEND-E2401 | error | Unsupported backend target | Set `plugin.default` to an installed plugin target. |
| BACKEND-E2402 | error | Unsupported backend capability | Choose a supported operation or use a hook. |
| PYSPARK-E2710 | error | Spark runtime is outside the configured profile | Select a profile containing both the PySpark client and Spark engine versions, or use the documented `log`/`off` override. |
| PYSPARK-E2711 | error | Spark runtime version is unavailable | Ensure PySpark is installed and the supplied Spark session exposes `version`; see runtime profile guidance. |
| CONNECT-E2601 | error | Spark Connect boundary is unsupported | Use Spark Connect DataFrame APIs or set `plugin.pyspark.variant = "ordinary"`. |
| CLI-X1101 | internal | Unexpected internal failure | Rerun with debug output and report the code with a reproduction. |
| STREAM-E0801 | error | Transform is not streaming-compatible | Keep the transform batch-only, add the required watermark or event-time bound, or rewrite the operation using a supported streaming shape. |
| STREAM-E0802 | error | Streaming output is not accepted by downstream input | Declare the downstream input with `streaming=True`, or explicitly allow the stream-to-batch boundary with `allow_stream_to_batch=True`. |
| STREAM-W0801 | warning | Hook streaming compatibility is unknown | Mark the hook `streaming=True` only after verifying it. |
| STREAM-W0802 | warning | Streaming aggregate state is unbounded | Use an event-time or session window with a matching watermark, or accept the caller-owned unbounded-state policy. |
| STREAM-W0803 | warning | Streaming partition parallelism is caller-tuned | Treat the count as deployment tuning, not stable partition identity. |
| STREAM-W0804 | warning | Streaming repartitioning is caller-tuned | Treat the count and hash keys as deployment tuning, not stable ordering or partition identity. |

## Reading Source Annotations

Compiler diagnostics retain the usual context, problem, fix, and documentation link. When Structure can locate the
relevant source safely, its `Source:` section includes a project-relative path, source excerpt, and caret pointing at
the primary location. A labelled `:::` location identifies related source, such as the declaration that makes an
assignment invalid. If source text is unavailable, Structure shows the logical source name instead; the diagnostic
code and suggested fix remain the same.

## Anchors

### CONF-E0101

Remove an unknown key or correct its spelling. See the [configuration reference](reference/ConfigSchema.ref.md).

### CONF-E0102

Use the setting's declared type and allowed values. See the [configuration reference](reference/ConfigSchema.ref.md).

### DSL-E0401

The step uses Python logic that cannot become a typed Spark expression. Use a supported expression or an explicit
[UDF](reference/UDF.ref.md) or [hook](reference/Transform.ref.md#hooks) for opaque runtime logic.

### DSL-E0402

Check transform decoration, method annotations, schema flow, and output bindings against the
[Transform reference](reference/Transform.ref.md).

### DSL-E0404

Compiled logic reached code marked `@special(type="ignore")`. Keep it outside compiled expressions or use an
explicit [UDF or hook](reference/UDF.ref.md#scalar-python-udf) when runtime execution is required.

### DSL-E0405

`@special(type="opaque")` code was called from a compiled step. Move the call outside compiler-visible logic, use
`@special(type="udf")` for scalar Python execution, or use an explicit hook for DataFrame logic.

### DSL-E0406

Declare the consumed row type with `sink(Schema)`, reference it as `self.sink_name` in `foreach(row, self.sink_name)`,
and capture a row returned by that step and mapped to a declared final output. See the
[row-level foreach specification](dev/specifications/V11RetainedV9DesignGates.spec.md#row-level-foreach).

### DSL-W0403

A Python UDF body is opaque to Spark optimization. Prefer typed expressions, or retain the explicit callback when
needed. See [scalar UDFs](reference/UDF.ref.md#scalar-python-udf) for the warning policy.

### PYSPARK-W2701

See [the driver-heap gotcha](troubleshooting/memory/spark_driver_heap_oom.gotcha.md), the
[Memory troubleshooting summary](troubleshooting/memory/Memory.trbl.md), and the
[resolved OOM issue](dev/issues/I09272602.Spark-driver-heap-exhaustion.issue.md).

The warning uses three distinct remedies. **Diminish** means Structure fused an eligible projection-union branch and
reduced one measured lineage multiplier; the remaining self-join can still grow exponentially. **Bound** means an
explicit `checkpoint()` or `local_checkpoint()` truncates the logical lineage at the selected source position.
**Remove** means the user restructures the algorithm so each round uses a stable base relation or another equivalent
bounded reduction. A cache, persist, alias, Python variable, or temporary view is not a lineage boundary, and a larger
driver heap only postpones the threshold.

### PYSPARK-W2702

See the [driver-heap gotcha](troubleshooting/memory/spark_driver_heap_oom.gotcha.md). One costly query plan branch feeds
multiple consumers. Checkpoint the shared branch, or combine consumers so Spark does not rebuild the same large query
plan repeatedly.

### PYSPARK-W2703

See [development troubleshooting](dev/Troubleshooting.md). Move strict checks to input/output boundaries, reduce
duplicate checks, or checkpoint before validating a reused branch.

### PYSPARK-W2704

See the [driver-heap gotcha](troubleshooting/memory/spark_driver_heap_oom.gotcha.md). Add a planning boundary before
the expensive output path or split the output computation into smaller stages.

### PYSPARK-W2705

Spark ignores a parsing pattern when its input is already a Date or LTZ Timestamp. Remove `format=`, or provide a
String expression containing the text to parse.

### PYSPARK-W2706

`make_timestamp(...)` with `spark.sql.timestampType=TIMESTAMP_NTZ` builds a wall-clock timestamp; the `timezone=`
argument does not shift that value. Remove it, or use `make_timestamp_ltz(...)` when you need an instant.

### PYSPARK-E2710

Structure checks the installed `pyspark.__version__` and the supplied session's public `spark.version` before compiling
an executed transform. Both must fall within `[tool.structure.plugin.pyspark].profile`. A broad profile such as
`>=3.5,<4.2` admits common features across those releases; it does not enable operations gated to exact 4.1. For a
4.1-only operation, select `profile = ">=4.1,<4.2"` and a supported `variant` in that TOML table, then use a matching
runtime. Static checks and compilation do not inspect the installed runtime.

The default `runtime_version_check = "raise"` raises before execution-time compilation, schema validation, hooks, or
table effects. Set it to `"log"` to emit an `ERROR` diagnostic and continue, or `"off"` to skip client and engine
version discovery. These are deliberate
overrides; they do not change static capability gates or establish compatibility for an otherwise unsupported
runtime. See [configuration](Configuration.md#pyspark-runtime-version-check) and
[runtime troubleshooting](../Troubleshooting.md#spark-runtime-profile-does-not-match).

### PYSPARK-E2711

The same runtime profile troubleshooting applies when client or engine version discovery fails or returns an
unparseable value. Check the PySpark installation and the supplied session's public `version` property.

### PYSPARK-W2707

On Spark 4.1, Sedona SQL extensions and native Spark geospatial functions register overlapping unqualified `st_*`
names. A transform containing both root Apache native helpers and `sedona.*` helpers can resolve a call through the
other
provider's registration. Use separate Spark sessions. For a handoff, materialize WKB and SRID in the source session,
then read both values and reconstruct the spatial value in the destination session. See the [cross-session WKB
recipe](recipes/GeospatialProviderBridge.md).

### SCHEMA-E0301

An expression may be null but the output field is required. Guard it with `where(value.is_not_null())` or supply
a non-null default. See [schema validation](reference/Schema.ref.md).

### SCHEMA-E0302

Use an explicit typed conversion or parsing helper for the output assignment. See
[expression types](reference/Expressions.ref.md#types-and-reusable-expressions).

### SCHEMA-E0303

The output expression type does not match its field. Choose a compatible expression or an explicit supported
conversion; see the [Schema reference](reference/Schema.ref.md).

### JOIN-E0601

A lookup condition must use supported equality-key pairs combined with `&`. Choose a relation join for a broader
condition; see the [Join reference](reference/Join.ref.md).

### JOIN-W0601

The lookup's right-side key is not proven unique. Declare a deterministic dedupe rule, or choose `left_join` or
`inner_join` when row multiplication is intended. See the [Join reference](reference/Join.ref.md).

### REL-E0701

See [Diagnostics.md](background/Diagnostics.back.md#rel-e0701).

### REL-E0702

See [Diagnostics.md](background/Diagnostics.back.md#rel-e0702).

### REL-E0703

See [Diagnostics.md](background/Diagnostics.back.md#rel-e0703).

### REL-E0704

See [Diagnostics.md](background/Diagnostics.back.md#rel-e0704).

### REL-E0705

See [Diagnostics.md](background/Diagnostics.back.md#rel-e0705).

### REL-E0706

See [Diagnostics.md](background/Diagnostics.back.md#rel-e0706).

### GEN-E0901

Run `structure compile`, review the generated diff, and commit the refreshed artifacts. See
[generated files](reference/Transform.ref.md#generated-files).

### GEN-E0902

Compile the transform and make its generated source root importable, or select `execution_mode = "online"`.
See [execution modes](reference/Execution.ref.md#execution-modes).

### GEN-E0903

The embedded hook depends on source state that the generated module cannot package. Use a standalone hook body
with local imports, or remove `embed_hooks`; see [generated options](Configuration.md#generated-code-options).

### ONLINE-E1201

Pass every declared input by name before calling `run(session)`. See
[input binding](reference/Execution.ref.md#input-binding-and-lane-flow).

### ONLINE-E1202

Supply a caller-owned Spark session to `StructureSession` or an explicitly configured custom executor.
See [session responsibilities](reference/Execution.ref.md#session-responsibilities).

### ONLINE-E1203

Select `execution_mode = "online"` or `"generated"`. See
[execution modes](reference/Execution.ref.md#execution-modes).

### BACKEND-E2401

Select an installed target with `plugin.default` or workflow `target=`, such as the bundled `pyspark` plugin.
See [plugin selection](reference/ConfigSchema.ref.md#plugin-and-target-selection).

### BACKEND-E2402

The configured profile or variant does not admit the requested operation. Follow the diagnostic's feature-specific
remedy. A supported 4.1 feature under the broad default requires `profile = ">=4.1,<4.2"` and an admitted `variant`
in `[tool.structure.plugin.pyspark]`; see [profile selection](reference/ConfigSchema.ref.md#pyspark-profiles).
Unimplemented features and missing providers retain their own design or dependency remedies.

### CONNECT-E2601

See [SparkConnect.md](background/SparkConnect.back.md#runtime-boundaries).

### CLI-X1101

An unclassified internal failure occurred. Report the diagnostic code, exact command, and a concise reproduction.
See the [CLI reference](reference/CLI.ref.md).

### STREAM-E0801

See [streaming diagnostics](reference/Streaming.ref.md#correct-streaming-diagnostics).

The transform step contains an operation that is not compatible with streaming execution. Keep the transform
batch-only, add the required watermark or event-time bound, or rewrite the operation using a supported streaming
shape. Keep sources, sinks, checkpoints, triggers, output modes, query lifecycle, and side effects in caller-owned
PySpark code. See [Streaming API](api/Streaming.api.md) and
[Spark Streaming](dev/specifications/SparkStreaming.spec.md).

### STREAM-E0802

See [streaming diagnostics](reference/Streaming.ref.md#correct-streaming-diagnostics).

A streaming stage output is consumed by a downstream input that does not accept streaming data. Declare the downstream
input with `streaming=True`, or explicitly allow the stream-to-batch boundary with `allow_stream_to_batch=True`.
Under default policy, compiler-visible compatible code is accepted automatically; unknown code still produces this
diagnostic. An explicit `streaming=False` always remains an error.

### STREAM-W0801

See [streaming diagnostics](reference/Streaming.ref.md#correct-streaming-diagnostics).

Structure cannot prove an arbitrary hook is streaming-compatible. Mark the hook `streaming=True` only when it returns a
DataFrame and avoids Spark actions, RDD/Pandas conversion, `readStream`, `writeStream`, query start/stop, checkpoints,
triggers, output-mode application, `foreach`, `foreachBatch`, and unmodeled stateful streaming operations. Keep
lifecycle code in caller-owned PySpark recipes such as `examples/streams/adoption.py`.

### STREAM-W0802

See [streaming diagnostics](reference/Streaming.ref.md#correct-streaming-diagnostics).

Spark permits ordinary business-key streaming aggregates with `update` or `complete` output mode, but a watermark
does not evict their state because the grouping is not event-time bounded. Use an event-time or session window when
state must be bounded, or explicitly accept the caller-owned unbounded-state policy.

### STREAM-W0803

See [Diagnostics background](background/Diagnostics.back.md).

Relation coalescing changes streaming task parallelism and can affect throughput. Treat the count as a deployment
tuning choice; it does not establish stable output order or partition identity.

### STREAM-W0804

Hash repartitioning shuffles streaming rows and can affect throughput. Treat the partition count and hash keys as
deployment tuning choices; they do not establish stable output order or partition identity.

### STREAM-W0805

A stateful streaming operation has no explicit state budget declaration. Add `budget(max_rows=..., max_state_bytes=...)`
immediately after the operation to declare its limits, or declare `memory_budget = budget(memory_source=...)` on the
transform when the deployment provides a shared memory policy. Compile-time checking reports declarations; runtime
enforcement requires an attached state budget guard and Spark progress metrics that can be mapped to each operator.

### CRYPTO-W0801

An AES encryption expression uses a caller-supplied IV. Prefer Spark's generated IV, or prove that the supplied IV is
unique for every encryption operation under the same key.

### SKETCH-W0802

An HLL union permits sketches with different `lg_config_k` precision. Use matching precision unless the resulting
precision trade-off is intentional.
