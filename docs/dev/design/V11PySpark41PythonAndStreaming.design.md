# V11 PySpark 4.1 Python and Streaming Design

## Purpose

Classify PySpark 4.1 Python execution and state APIs honestly while preserving Structure's symbolic and caller-owned
streaming boundaries.

## Arrow UDF and UDTF

Arrow-native UDF and UDTF decorators execute user Python in workers and introduce serialization, dependency, batching,
error, and (for UDTFs) row-cardinality behavior. They remain explicit raw or caller-owned boundaries in V11. A future
narrow typed contract must specify input/output schemas, nullability, batching, failure, resource, and Connect rules.

## Row-based transformWithState

Structure implements `transform_with_state(...)` for ordinary PySpark `>=4.1,<4.2`. It captures a typed processor
declared by inheriting `StateProcessor[Input, Key, State, Output]` (with optional `@state_processor` validation), or an opaque native PySpark processor
bound with `external_state_processor(...)`. Typed callbacks use one `ValueState` and timer operations; processor bodies
remain ordinary worker Python. The compiler lowers the operation to a shared recipe used by online and generated
execution and classifies it as one stateful stage.

The row operation is not a general callback or query-lifecycle escape hatch. It has explicit composition and schema rules;
Spark Connect and PySpark 4.0 row execution are not claimed. Positive support remains gated until the ordinary 4.1 runtime
proves typed and native behavior, timers, online/generated parity, and same-checkpoint restart. The separate
`transform_with_state_in_pandas(...)` operation covers the Pandas API on ordinary PySpark 4.0 and 4.1 and has its own
dependency and runtime evidence gate. Both state operations require pandas, PyArrow, and Protobuf in PySpark 4.1 driver
and worker environments; the Pandas operation requires them on PySpark 4.0 as well. `applyInPandasWithState` remains
outside the Structure compiler surface.

The initial ordinary PySpark 4.1 integration lane selects the backend version check and V11 integration tests only. It
does not run pre-V11 integration or concept suites against 4.1, and it does not establish a support claim by itself.
Its image pins Protobuf 6.33.0 to match the generated Spark state protocol; the 3.5 and 4.0 images keep Protobuf 5.29.3.
The V11 test session selects RocksDB because the default HDFS-backed provider rejects TransformWithState's multiple
column families. The row and Pandas plans own separate acceptance evidence for their API interface and each claimed
profile.

Typed processor callbacks receive `TimerContext` for timer registration/deletion/listing plus the current trigger's
processing time and watermark in milliseconds. The current watermark is available only when the streaming plan has a
watermark. The row adapter validates the required callback and optional timer callback signatures before calling Spark,
converts rows and state through their declared Schemas, and checks output Schema type and non-null fields before yielding
rows.

The typed API stays intentionally narrower than Spark's native processor interface: one `ValueState`, timer operations,
typed input/key/state/output values, and ordinary Python callbacks. The opaque native processor is the full-parity path for
`ListState`, `MapState`, multiple named variables, TTL, initial-state handling, and other Spark APIs. After the first
typed/native runtime slice passes, revisit typed multi-field keys, collection states, TTL, and typed initial state as
separate contracts with schema, nullability, state naming, checkpoint compatibility, and migration rules. Do not add a
typed `close` callback or state-schema evolution in this phase: Spark worker cleanup is not guaranteed after failure, and
checkpoint schema evolution needs an explicit migration contract.

## Acceptance

Unsupported profiles and compositions produce stable diagnostics naming the required target or state-stage constraint.
Generated-source scans verify that only the explicit state operations are emitted and that generated transforms do not
own sources, sinks, checkpoints, or query lifecycle.
