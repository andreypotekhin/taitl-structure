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
| DSL-W0403 | warning | Python UDF is optimizer-opaque | Keep intentional UDFs or set `warn_on_udfs = false`. |
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
| CONNECT-E2601 | error | Spark Connect boundary is unsupported | Use Spark Connect DataFrame APIs or set `plugin.pyspark.variant = "ordinary"`. |
| CLI-X1101 | internal | Unexpected internal failure | Rerun with debug output and report the code with a reproduction. |
| STREAM-E0801 | error | Transform is not streaming-compatible | Keep the transform batch-only, add the required watermark or event-time bound, or rewrite the operation using a supported streaming shape. |
| STREAM-E0802 | error | Streaming output is not accepted by downstream input | Declare the downstream input with `streaming=True`, or explicitly allow the stream-to-batch boundary with `allow_stream_to_batch=True`. |
| STREAM-W0801 | warning | Hook streaming compatibility is unknown | Mark the hook `streaming=True` only after verifying it. |
| STREAM-W0802 | warning | Streaming aggregate state is unbounded | Use an event-time or session window with a matching watermark, or accept the caller-owned unbounded-state policy. |

## Reading Source Annotations

Compiler diagnostics retain the usual context, problem, fix, and documentation link. When Structure can locate the
relevant source safely, its `Source:` section includes a project-relative path, source excerpt, and caret pointing at
the primary location. A labelled `:::` location identifies related source, such as the declaration that makes an
assignment invalid. If source text is unavailable, Structure shows the logical source name instead; the diagnostic
code and suggested fix remain the same.

## Anchors

### CONF-E0101
See [Diagnostics.md](background/Diagnostics.back.md#conf-e0101).

### CONF-E0102
See [Diagnostics.md](background/Diagnostics.back.md#conf-e0102).

### DSL-E0401
See [Diagnostics.md](background/Diagnostics.back.md#dsl-e0401).

### DSL-E0402
See [Diagnostics.md](background/Diagnostics.back.md#dsl-e0402).

### DSL-E0404
See [Diagnostics.md](background/Diagnostics.back.md#dsl-e0404).

### DSL-W0403
See [Diagnostics.md](background/Diagnostics.back.md#dsl-w0403).

### SCHEMA-E0301
See [Diagnostics.md](background/Diagnostics.back.md#schema-e0301).

### SCHEMA-E0302
See [Diagnostics.md](background/Diagnostics.back.md#schema-e0302).

### SCHEMA-E0303
See [Diagnostics.md](background/Diagnostics.back.md#schema-e0303).

### JOIN-E0601
See [Diagnostics.md](background/Diagnostics.back.md#join-e0601).

### JOIN-W0601
See [Diagnostics.md](background/Diagnostics.back.md#join-w0601).

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
See [Diagnostics.md](background/Diagnostics.back.md#gen-e0901).

### GEN-E0902
See [Diagnostics.md](background/Diagnostics.back.md#gen-e0902).

### GEN-E0903
See [Diagnostics.md](background/Diagnostics.back.md#gen-e0903).

### ONLINE-E1201
See [Diagnostics.md](background/Diagnostics.back.md#online-e1201).

### ONLINE-E1202
See [Diagnostics.md](background/Diagnostics.back.md#online-e1202).

### ONLINE-E1203
See [Diagnostics.md](background/Diagnostics.back.md#online-e1203).

### BACKEND-E2401
See [Diagnostics.md](background/Diagnostics.back.md#backend-e2401).

### BACKEND-E2402
See [Diagnostics.md](background/Diagnostics.back.md#backend-e2402).

### CONNECT-E2601
See [SparkConnect.md](background/SparkConnect.back.md#runtime-boundaries).

### CLI-X1101
See [Diagnostics.md](background/Diagnostics.back.md#cli-x1101).

### STREAM-E0801
See [Diagnostics.md](background/Diagnostics.back.md#stream-e0801).

The transform step contains an operation that is not compatible with streaming execution. Keep the transform
batch-only, add the required watermark or event-time bound, or rewrite the operation using a supported streaming
shape. Keep sources, sinks, checkpoints, triggers, output modes, query lifecycle, and side effects in caller-owned
PySpark code. See [Streaming API](api/Streaming.api.md) and
[Spark Streaming](dev/specifications/SparkStreaming.md).

### STREAM-E0802

See [Diagnostics.md](background/Diagnostics.back.md#stream-e0802).

A streaming stage output is consumed by a downstream input that does not accept streaming data. Declare the downstream
input with `streaming=True`, or explicitly allow the stream-to-batch boundary with `allow_stream_to_batch=True`.
Under default policy, compiler-visible compatible code is accepted automatically; unknown code still produces this
diagnostic. An explicit `streaming=False` always remains an error.

### STREAM-W0801
See [Diagnostics.md](background/Diagnostics.back.md#stream-w0801).

Structure cannot prove an arbitrary hook is streaming-compatible. Mark the hook `streaming=True` only when it returns a
DataFrame and avoids Spark actions, RDD/Pandas conversion, `readStream`, `writeStream`, query start/stop, checkpoints,
triggers, output-mode application, `foreach`, `foreachBatch`, and unmodeled stateful streaming operations. Keep
lifecycle code in caller-owned PySpark recipes such as `examples/streams/adoption.py`.

### STREAM-W0802

See [Diagnostics.md](background/Diagnostics.back.md#stream-w0802).

Spark permits ordinary business-key streaming aggregates with `update` or `complete` output mode, but a watermark
does not evict their state because the grouping is not event-time bounded. Use an event-time or session window when
state must be bounded, or explicitly accept the caller-owned unbounded-state policy.
