# Sketches and Bitmaps

Sketches and bitmaps are compact state values for approximate cardinality and membership-style metrics. Structure
keeps that state typed: an HLL value is not ordinary bytes, and a Bitmap value is not an HLL value. Spark stores the
values as Binary, while the Structure schema preserves their meaning for later aggregate and scalar operations.

Use this topic when a transform needs to publish a reusable approximate-distinct summary or a compact set summary.
For ordinary exact counts, use `count_distinct(...)`; for a one-off approximate count, consider
`approx_count_distinct(...)`. Choose a sketch only when its opaque state is itself a useful output.

## Two shapes

```text
events -> grouped sketch construction -> typed sketch field -> scalar estimate/count
events -> grouped sketch construction -> typed sketch field -> later compatible merge
```

Construction and merge aggregates change row grain in the same way as other grouped metrics. Estimating an HLL or
counting a Bitmap reads one opaque value and preserves the current row.

```python
group_by(tenant_id=event.tenant_id)
return TenantSketches(
    tenant_id=event.tenant_id,
    customer_hll=hll_sketch_agg(event.customer_id, lg_config_k=12),
    feature_bitmap=bitmap_construct_agg(event.feature_id),
)
```

The resulting fields are typed `hll_sketch(lg_config_k=12)` and `bitmap()` values. A later transform can estimate the
first with `hll_sketch_estimate(...)` and count the second with `bitmap_count(...)`.

## HLL precision is part of the contract

Keep the same `lg_config_k` wherever HLL values will be merged. `hll_union(left, right)` rejects a mismatch before
execution. If an external protocol requires a mixed-precision union, pass
`allow_different_lg_config_k=True` deliberately; Structure emits `SKETCH-W0802` because Spark can reduce precision.

Do not use an HLL estimate as an exact count. It is an approximation, and its expected accuracy depends on the chosen
precision and the target runtime. Preserve the chosen precision in the schema and in application documentation.

## Bitmap positions

Bitmap construction accepts integer positions. `bitmap_bit_position(...)` and `bitmap_bucket_number(...)` help map a
numeric identifier to the positions expected by Spark's bitmap functions. They are arithmetic helpers; they do not
turn arbitrary Strings or Binary values into a Bitmap.

## Persistence and targets

Opaque state can be carried through a declared Structure Schema, but its Binary representation is not a portable file
or cross-engine interchange format. Pin and test the compatible Spark/profile combination whenever state outlives the
transform that created it. Use an explicit application-owned conversion or native PySpark boundary for external
interchange.

HLL and Bitmap are part of the default baseline. KLL and Theta are profile-gated work for PySpark 4.1; Count-Min and
observation metrics remain caller-owned. Grouped sketch aggregation in streaming follows the usual watermark and
output-mode rules; Structure does not own the source, sink, checkpoint, or retention policy.

See the [Sketches and Bitmaps API](../api/Sketches.api.md),
[Sketches and Bitmaps reference](../reference/Sketches.ref.md), and
[Sketch and Bitmap Metrics recipe](../recipes/SketchBitmapMetrics.md).
