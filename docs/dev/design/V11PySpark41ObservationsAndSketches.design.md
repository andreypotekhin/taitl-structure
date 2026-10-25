# V11 PySpark 4.1 Observations and Sketches Design

## Purpose

Extend the canonical [PySpark SQL boundary contracts](PySparkSQLBoundaryContracts.design.md) with the PySpark 4.1
sketch profile. The broader platform begins with HLL and Bitmap in the default baseline, then adds KLL and Theta only
when their PySpark 4.1 capabilities and evidence are available.

## Observation boundary

The typed batch observation design agreed on 2026-10-18 is recorded in the
[completed observation ExecPlan](../past/planning/P10182602.V11-batch-observations.plan.md). It resolves the API,
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

KLL, Theta, HLL, and Bitmap values use logical types backed by Spark Binary. The `structure.logical_type` field metadata
stores a versioned Structure descriptor, recursively across schemas. It records sketch kind and identity-bearing
parameters, never runtime profile or operation precision. Missing metadata may be supplied by an explicit schema;
malformed, unsupported-version, or conflicting metadata is an error. Untagged Binary remains Binary unless the caller
declares it as a sketch. Metadata describes intended type, not buffer validity or cross-library portability.

KLL declarations retain an explicit Long, Float, or Double domain. `kll_sketch_agg` infers Integer/Long as Long and
Float/Double as their corresponding domains; Decimal requires an explicit cast. Six generic KLL helpers map to Spark's
domain-specific aggregate, count, quantile, rank, pairwise merge, and string functions. Construction `k` defaults to
200 and is operation configuration, not type identity. Spark 4.1.0 has no KLL merge aggregate.

Theta uses one input-domain-independent type. Seven helpers provide construction, union, intersection, directional
difference, and Long estimate operations. `lg_nom_entries` defaults to 12 and is operation configuration. Neither
helper family converts incompatible sketch domains or families. Empty KLL quantile/rank queries preserve Spark's native
error; callers can guard with the sketch count. No safe variant is provided.

HLL retains its `lg_config_k` identity and mixed-precision opt-in policy; Bitmap and Theta identity contain only kind.
The former sketch factory `profile=` argument is removed; target support belongs to `plugin.pyspark.profile` and
capability admission. Batch KLL/Theta parity is evidenced for exact `>=4.1,<4.2` ordinary and Connect profiles.
Ordinary PySpark 4.1 also passes watermarked KLL/Theta aggregate parity and same-configuration checkpoint restart.
Spark Connect remains batch-only under the existing Structured Streaming boundary. Existing HLL/Bitmap admission
remains unchanged. Streaming follows existing grouped aggregate, watermark, output-mode, and caller-owned checkpoint
rules. See
[the KLL/Theta implementation ExecPlan](../past/planning/P10182603.V11-kll-theta-sketches.plan.md).
