# V11 PySpark 4.1 Python and Streaming Design

## Purpose

Classify PySpark 4.1 Python execution and state APIs honestly while preserving Structure's symbolic and caller-owned
streaming boundaries.

## Arrow UDF and UDTF

Arrow-native UDF and UDTF decorators execute user Python in workers and introduce serialization, dependency, batching,
error, and (for UDTFs) row-cardinality behavior. They remain explicit raw or caller-owned boundaries in V11. A future
narrow typed contract must specify input/output schemas, nullability, batching, failure, resource, and Connect rules.

## Row-based transformWithState

`transformWithState` owns user-defined state, initialization, timers or timeouts, checkpoint recovery, and output
cardinality. It is not admitted by the existing one-stateful-operation policy. V11 documents a design gate, the
caller-owned recipe boundary, and the evidence required for a future promotion; it does not generate state stores or
streaming lifecycle code.

## Acceptance

Unsupported use produces a stable capability/diagnostic message naming the API family, why it is outside the current
contract, and the caller-owned PySpark alternative. Generated-source scans prove that no gated Python or state API is
emitted accidentally.
