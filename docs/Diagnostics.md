# Diagnostics

Below is an index for published diagnostic codes. Ror full diagnostic contract see
[docs/specifications/Diagnostics.md](docs/specifications/Diagnostics.md).

## Active Codes

| Code | Severity | Title | Use |
| --- | --- | --- | --- |
| CONF-E0101 | error | Unknown configuration key | Remove the key or correct its spelling. |
| CONF-E0102 | error | Invalid configuration value | Set the value to one of the allowed values. |
| DSL-E0401 | error | Unsupported symbolic expression | Use Structure DSL helpers, an `@expr_fn` helper, or a hook. |
| DSL-E0402 | error | Invalid transform structure | Check decoration, annotations, schema flow, and output fields. |
| SCHEMA-E0301 | error | Nullable expression assigned to non-nullable field | Guard the value or provide a non-null default. |
| SCHEMA-E0302 | error | Explicit conversion required | Use an explicit conversion helper such as `to_decimal(...)`. |
| SCHEMA-E0303 | error | Incompatible output field type | Use a compatible expression type or explicit conversion. |
| JOIN-E0601 | error | Unsupported join condition | Use equality pairs with `==` or `null_safe_eq(...)` and combine them with `&`. |
| JOIN-W0601 | warning | join_one uniqueness is not proven | Mark the joined key `primary_key=True` or use `join_many(...)` in v2. |
| GEN-E0901 | error | Generated output is stale | Run `structure compile` and commit the generated changes. |
| GEN-E0902 | error | Generated transform is not importable | Rebuild generated code or use `execution_mode = "online"`. |
| ONLINE-E1201 | error | Transform input is missing | Pass every declared input DataFrame before `run(session)`. |
| ONLINE-E1202 | error | Online PySpark runner is not configured | Pass an online executor or use `execution_mode = "generated"`. |
| ONLINE-E1203 | error | Execution mode is unsupported | Use `execution_mode = "online"` or `execution_mode = "generated"`. |
| BACKEND-E2401 | error | Unsupported backend target | Set `target_backend = "pyspark"` for v1. |
| BACKEND-E2402 | error | Unsupported backend capability | Choose a supported operation or use a hook. |
| CLI-X1101 | internal | Unexpected internal failure | Rerun with debug output and report the code with a reproduction. |
| STREAM-E0801 | error | Transform is not streaming-compatible | Keep the transform batch-only or rewrite the unsupported shape. |
| STREAM-W0801 | warning | Hook streaming compatibility is unknown | Mark the hook `streaming_safe=True` only after verifying it. |

## Anchors

### CONF-E0101
See [the full specification](docs/specifications/Diagnostics.md#conf-e0101).

### CONF-E0102
See [the full specification](docs/specifications/Diagnostics.md#conf-e0102).

### DSL-E0401
See [the full specification](docs/specifications/Diagnostics.md#dsl-e0401).

### DSL-E0402
See [the full specification](docs/specifications/Diagnostics.md#dsl-e0402).

### SCHEMA-E0301
See [the full specification](docs/specifications/Diagnostics.md#schema-e0301).

### SCHEMA-E0302
See [the full specification](docs/specifications/Diagnostics.md#schema-e0302).

### SCHEMA-E0303
See [the full specification](docs/specifications/Diagnostics.md#schema-e0303).

### JOIN-E0601
See [the full specification](docs/specifications/Diagnostics.md#join-e0601).

### JOIN-W0601
See [the full specification](docs/specifications/Diagnostics.md#join-w0601).

### GEN-E0901
See [the full specification](docs/specifications/Diagnostics.md#gen-e0901).

### GEN-E0902
See [the full specification](docs/specifications/Diagnostics.md#gen-e0902).

### ONLINE-E1201
See [the full specification](docs/specifications/Diagnostics.md#online-e1201).

### ONLINE-E1202
See [the full specification](docs/specifications/Diagnostics.md#online-e1202).

### ONLINE-E1203
See [the full specification](docs/specifications/Diagnostics.md#online-e1203).

### BACKEND-E2401
See [the full specification](docs/specifications/Diagnostics.md#backend-e2401).

### BACKEND-E2402
See [the full specification](docs/specifications/Diagnostics.md#backend-e2402).

### CLI-X1101
See [the full specification](docs/specifications/Diagnostics.md#cli-x1101).

### STREAM-E0801
See [the full specification](docs/specifications/Diagnostics.md#stream-e0801).

### STREAM-W0801
See [the full specification](docs/specifications/Diagnostics.md#stream-w0801).
