# Sketch and Bitmap Metrics

**Problem:** An event stream has too many customer IDs and feature IDs to retain in every tenant summary, but later
steps need an approximate customer count and the number of represented feature positions.

**Solution:** Build typed HLL and Bitmap fields in one grouped summary, then consume those fields in a later transform.
The fields retain their opaque meaning; they are not converted to ordinary Binary just because Spark stores them that
way.

## Define the contracts

```python
from structure import *
from structure.plugin.pyspark import *


class Event(Schema):
    tenant_id = string(nullable=False)
    customer_id = string(nullable=False)
    feature_id = long(nullable=False)


class TenantSketches(Schema):
    tenant_id = string(nullable=False)
    customer_hll = hll_sketch(lg_config_k=12, nullable=True)
    feature_bitmap = bitmap(nullable=True)


class TenantMetrics(Schema):
    tenant_id = string(nullable=False)
    estimated_customers = long(nullable=True)
    represented_features = long(nullable=True)
```

The `lg_config_k=12` declaration is part of the HLL contract. Reuse it for any HLL value that may later be merged.

## Build reusable state

```python
@transform
class SummarizeTenantEvents(Transform):
    events = input(Event)
    sketches = output(TenantSketches)

    def summarize(self, event: Event) -> TenantSketches:
        group_by(tenant_id=event.tenant_id)
        return TenantSketches(
            tenant_id=event.tenant_id,
            customer_hll=hll_sketch_agg(event.customer_id, lg_config_k=12),
            feature_bitmap=bitmap_construct_agg(event.feature_id),
        )
```

This is a grouped aggregate: the output has one row per tenant. `hll_sketch_agg(...)` builds opaque state for an
approximate distinct-customer estimate; `bitmap_construct_agg(...)` collects integral feature positions into a Bitmap.

## Consume the state without losing its type

```python
@transform
class PublishTenantMetrics(Transform):
    sketches = input(TenantSketches)
    metrics = output(TenantMetrics)

    def publish(self, summary: TenantSketches) -> TenantMetrics:
        return TenantMetrics(
            tenant_id=summary.tenant_id,
            estimated_customers=hll_sketch_estimate(summary.customer_hll),
            represented_features=bitmap_count(summary.feature_bitmap),
        )
```

Both scalar consumers preserve one row per tenant. They do not read input data again or materialize a Python
collection.

## Merge HLL values deliberately

When two HLL fields have the same declared `lg_config_k`, use `hll_union(left, right)`. A mismatch fails by default.
Use `allow_different_lg_config_k=True` only for an intentional external compatibility case; it emits `SKETCH-W0802`
because Spark may reduce precision.

Do not persist these fields as a generic cross-engine Binary interchange format. The data remains Spark/profile
specific. For streaming summaries, also satisfy the ordinary grouped-aggregate watermark and output-mode requirements.

For the complete contracts, see the [Sketches and Bitmaps API](../api/Sketches.api.md) and
[Sketches and Bitmaps reference](../reference/Sketches.ref.md).
