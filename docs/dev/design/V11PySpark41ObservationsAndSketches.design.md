# V11 PySpark 4.1 Observations and Sketches Design

## Purpose

Extend the canonical [PySpark SQL boundary contracts](PySparkSQLBoundaryContracts.design.md) with the PySpark 4.1
sketch profile. The broader platform begins with HLL and Bitmap in the default baseline, then adds KLL and Theta only
when their PySpark 4.1 capabilities and evidence are available.

## Observation boundary

The typed batch observation design agreed on 2026-10-18 is recorded in the
[completed observation ExecPlan](../planning/past/P10182602.V11-batch-observations.plan.md). It resolves the API,
typing, retrieval, native lifecycle, and composition choices below. Structure supports batch observations on ordinary
and Spark Connect `>=4.1,<4.2`; focused native/online/generated parity passed on PySpark/Spark 4.1.0 for both variants.

An observation is a typed metric side channel attached to the current batch relation; it is not an output field. A
transform declares `quality = observation(OrderMetrics)` and records one `observe(OrderMetrics(...))` operation, or
uses `observe(self.quality, OrderMetrics(...))` when the schema is ambiguous. After an action traverses a returned
output that includes the observation, callers read `result.quality.get()` as a fresh `OrderMetrics` snapshot. The
operation preserves rows and schema, adds no action, supports scalar and complex metric values, and follows native
Spark lifecycle behavior: ordinary Spark retains first-action metrics while Connect reports its current native
metrics. Metrics are batch-only, and child handles follow `allow_stage_outputs`. Each variant is independently
capability-gated; earlier and later profiles remain unsupported.

## Sketch boundary

KLL and Theta functions extend the shared opaque-state model. They use distinct Structure types backed by Spark Binary,
cannot be cast to ordinary Binary or to another sketch kind, and are available only through their PySpark 4.1 profile.
KLL types retain their numeric domain; Theta supports its typed union, intersection, difference, and estimate forms.
Support requires precision parameters, merge rules, deterministic fixtures, dependency diagnostics, and positive
ordinary-PySpark and Spark Connect evidence. They are not default-baseline claims.
