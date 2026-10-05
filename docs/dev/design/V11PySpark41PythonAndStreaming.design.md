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
declared with `StateProcessor[Input, Key, State, Output]` and `@state_processor`, or an opaque native PySpark processor
bound with `external_state_processor(...)`. Typed callbacks use one `ValueState` and timer operations; processor bodies
remain ordinary worker Python. The compiler lowers the operation to a shared recipe used by online and generated
execution and classifies it as one stateful stage.

The row operation is not a general callback or query-lifecycle escape hatch. It has explicit composition and schema rules;
Spark Connect and PySpark 4.0 row execution are not claimed. Positive support remains gated until the ordinary 4.1 runtime
proves typed and native behavior, timers, online/generated parity, and same-checkpoint restart. The separate
`transform_with_state_in_pandas(...)` operation covers the Pandas API on ordinary PySpark 4.0 and 4.1 and has its own
dependency and runtime evidence gate. `applyInPandasWithState` remains outside the Structure compiler surface.

## Acceptance

Unsupported profiles and compositions produce stable diagnostics naming the required target or state-stage constraint.
Generated-source scans verify that only the explicit state operations are emitted and that generated transforms do not
own sources, sinks, checkpoints, or query lifecycle.
