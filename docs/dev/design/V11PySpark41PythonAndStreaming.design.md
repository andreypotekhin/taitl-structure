# V11 PySpark 4.1 Python and Streaming Design

## Purpose

Classify PySpark 4.1 Python execution and state APIs honestly while preserving Structure's symbolic and caller-owned
streaming boundaries.

## Arrow UDF and UDTF

Arrow-native UDF and UDTF decorators execute user Python in workers and introduce serialization, dependency, batching,
error, and (for UDTFs) row-cardinality behavior. They remain explicit raw or caller-owned boundaries in V11. A future
narrow typed contract must specify input/output schemas, nullability, batching, failure, resource, and Connect rules.

## Row-based transformWithState

Structure supports `transform_with_state(...)` for ordinary PySpark `>=4.1,<4.2`. It captures a typed processor
declared by inheriting `StateProcessor[Input, Key, Output]` (with optional `@state_processor` validation), or an
opaque native PySpark processor bound with `external_state_processor(...)`. Typed processors declare named Value,
List, and Map states and access their wrappers through `self`; processor bodies remain ordinary worker Python. The compiler lowers the operation to a shared
recipe used by online and generated execution and classifies it as one stateful stage.

The row operation supports `Append` and `Update` output modes and `None`, `ProcessingTime`, and `EventTime` time modes
in their valid combinations. `Complete` is rejected during compilation. Ordinary 4.1 online/generated evidence covers
both output modes, all time modes, composite keys, processing/event-time timers, and same-checkpoint restart. Spark
Connect and PySpark 4.0 row execution are not claimed. Typed initial state requires a paired callback and input
relation. TTL requires ProcessingTime. Persisted state changes are checkpoint-sensitive; Structure does not migrate
state and recommends a new checkpoint after declaration changes. The row operation is not a general callback or
query-lifecycle escape hatch. See the [typed parity design](V11TransformWithStateTypedParity.design.md) for the
attribute and checkpoint contract. The separate
`transform_with_state_in_pandas(...)` operation covers the Pandas API on ordinary PySpark 4.0 and 4.1 and has its own
dependency and runtime evidence gate. Both state operations require pandas, PyArrow, and Protobuf in PySpark 4.1 driver
and worker environments; the Pandas operation requires them on PySpark 4.0 as well. The separate
`apply_in_pandas_with_state(...)` operation targets ordinary PySpark 3.5, 4.0, and 4.1 through the legacy
`GroupedData.applyInPandasWithState` contract; typed/native accumulation and same-checkpoint restart are evidenced on
the three pinned ordinary profiles, while timeout and zero/multiple-output behavior remain gated by its own plan.

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

The typed API stays narrower than Spark's native processor interface: named Value/List/Map state, ProcessingTime TTL,
typed initial state, timer operations, typed input/key/output values, and ordinary Python callbacks. The opaque native
processor remains the path for other Spark APIs and valid Python constructs that Structure does not compile. Do not add a
typed `close` callback or state-schema evolution in this phase: Spark worker cleanup is not guaranteed after failure, and
checkpoint schema evolution needs an explicit migration contract.

## Acceptance

Unsupported profiles and compositions produce stable diagnostics naming the required target or state-stage constraint.
Generated-source scans verify that only the explicit state operations are emitted and that generated transforms do not
own sources, sinks, checkpoints, or query lifecycle.
