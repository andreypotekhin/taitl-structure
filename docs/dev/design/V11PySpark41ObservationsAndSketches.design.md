# V11 PySpark 4.1 Observations and Sketches Design

## Purpose

Extend the canonical [PySpark SQL boundary contracts](PySparkSQLBoundaryContracts.design.md) with the PySpark 4.1
sketch profile. The broader platform begins with HLL and Bitmap in the default baseline, then adds KLL and Theta only
when their PySpark 4.1 capabilities and evidence are available.

## Observation boundary

An observation is a metric side channel attached to a DataFrame action or query; it is not automatically an output
field. It remains a separate, later design. That design must specify metric names, value types, retrieval timing,
action ordering, duplicate names, batch versus streaming behavior, and online/generated parity. Until then, the catalog
keeps observations caller-owned-guided and points users to a raw PySpark wrapper.

## Sketch boundary

KLL and Theta functions extend the shared opaque-state model. They use distinct Structure types backed by Spark Binary,
cannot be cast to ordinary Binary or to another sketch kind, and are available only through their PySpark 4.1 profile.
KLL types retain their numeric domain; Theta supports its typed union, intersection, difference, and estimate forms.
Support requires precision parameters, merge rules, deterministic fixtures, dependency diagnostics, and positive
ordinary-PySpark and Spark Connect evidence. They are not default-baseline claims.
