# V11 PySpark 4.1 Observations and Sketches Design

## Purpose

Decide whether PySpark 4.1 complex observations and approximate data-sketch aggregates belong in Structure's typed
transform contract or at a caller-owned boundary.

## Observation boundary

An observation is a metric side channel attached to a DataFrame action or query; it is not automatically an output field.
The design must specify metric names, value types, retrieval timing, action ordering, duplicate names, batch versus
streaming behavior, and online/generated parity. If Structure cannot preserve those semantics, the catalog keeps
observations caller-owned-guided and points users to a raw PySpark wrapper.

## Sketch boundary

KLL and Theta functions produce serialized approximate sketches and may require Spark-provided or optional sketch
libraries. Support requires a stable binary schema, mergeability rules, precision parameters, deterministic test
fixtures, dependency diagnostics, and a documented way to turn a sketch into a typed result. Until then, sketch
aggregates are design-gated rather than advertised as ordinary numeric aggregates.
