# Sketches and Bitmaps Reference

Use HLL and Bitmap fields when a transform needs to carry compact analytical state instead of only publishing a final
number. HLL represents approximate cardinality; Bitmap represents integral positions. Structure treats each as an
opaque value with a declared family, even though Spark physically stores Binary data.

For a one-off distinct count, prefer `count_distinct(...)` when an exact answer is practical, or
`approx_count_distinct(...)` when only the final approximation matters. Use a sketch when a later transform must
consume or combine the state itself.

## Declare state explicitly

```python
class TenantSketches(Schema):
    tenant_id = string(nullable=False)
    customers = hll_sketch(lg_config_k=12, nullable=True)
    features = bitmap(nullable=True)
```

The HLL precision belongs in the declaration. Choose one value for every HLL field that will be merged. Bitmap values
contain only integral positions. Neither value is ordinary `binary()` in the Structure API, so accidental use of a
serialized payload as a sketch is rejected before execution.

## Build and consume HLL values

```python
group_by(tenant_id=event.tenant_id)
return TenantSketches(
    tenant_id=event.tenant_id,
    customers=hll_sketch_agg(event.customer_id, lg_config_k=12),
    features=bitmap_construct_agg(event.feature_id),
)
```

`hll_sketch_agg(...)` and `bitmap_construct_agg(...)` are grouped metrics. The resulting state can be consumed in a
later row-preserving projection:

```python
estimated_customers = hll_sketch_estimate(summary.customers)
represented_features = bitmap_count(summary.features)
```

The estimates and counts are nullable because their inputs can be nullable and aggregate groups can have no qualifying
value.

## Merge compatible state

`hll_union(left, right)` accepts two HLL values of the same precision. It rejects a precision mismatch by default.
`allow_different_lg_config_k=True` is an opt-in for external compatibility and emits `SKETCH-W0802`; the target can
reduce the precision of the result. `bitmap_or_agg(...)` merges Bitmap values within an aggregate group.

Do not replace a merge with a Binary cast, and do not interpret an HLL estimate as an exact count. The library also
does not define cross-engine or cross-version Binary interchange. Keep Spark/profile compatibility under application
control whenever opaque state is persisted or transferred.

## Streaming and target profiles

Scalar HLL/Bitmap consumers preserve rows. Aggregate construction and Bitmap OR use the same watermark, state, and
output-mode rules as other grouped streaming aggregates. The transform boundary does not own a source, sink,
checkpoint, or retention policy.

HLL and Bitmap are default-baseline features. KLL and Theta declarations require the PySpark 4.1 profile and do not
imply a default-baseline aggregate API. Count-Min and observations remain caller-owned boundaries.

See the [Sketches and Bitmaps API](../api/Sketches.api.md), [Sketches background](../background/Sketches.back.md), and
[Sketch and Bitmap Metrics recipe](../recipes/SketchBitmapMetrics.md).
