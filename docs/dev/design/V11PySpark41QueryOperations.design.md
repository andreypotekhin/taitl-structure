# V11 PySpark 4.1 Query Operations Design

## Purpose

Define safe compiler-visible contracts for PySpark 4.1 subquery and lateral relation APIs without turning arbitrary
DataFrame callbacks into hidden code generation.

## Correlated existence

An existence predicate names its outer relation, inner relation, correlation keys, null policy, and expected boolean
result. Scope resolution is explicit: a field must belong to the declared relation alias, and a field from another
relation is rejected before lowering. Duplicate inner rows do not multiply the outer result.

## Lateral relations

`lateralJoin` is a relation-shape operation, so admission requires a declared output schema, cardinality class, join
kind, correlation scope, and streaming classification. The first implementation may support only a typed, bounded
relation expression; raw Python functions returning DataFrames remain a caller-owned hook. If Spark Connect differs,
the capability ledger records the difference instead of sharing an unsupported renderer.

## Evidence

Use positive and negative fixtures for correlated and uncorrelated existence, duplicate and empty inner inputs, null
keys, alias collisions, lateral zero/one/many output rows, and generated-source scope safety. Compare online and
generated results and inspect `structure explain` for relation and field dependencies.
