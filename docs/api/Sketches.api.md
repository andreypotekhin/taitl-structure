# Sketches and Bitmaps API

These helpers build and consume typed opaque HLL and Bitmap state. Spark represents the state as Binary, but Structure
retains the sketch family and HLL precision so only compatible operations compose.

## Schema Types

| Structure API | Spark representation | Example |
| --- | --- | --- |
| `hll_sketch(lg_config_k=12)` | Binary with HLL brand | `customers = hll_sketch(lg_config_k=12)` |
| `bitmap()` | Binary with Bitmap brand | `features = bitmap()` |
| `kll_sketch()` | Binary with KLL brand | Profile-gated PySpark 4.1 declaration |
| `theta_sketch()` | Binary with Theta brand | Profile-gated PySpark 4.1 declaration |

**Details And Differences**

- `hll_sketch(...)` accepts a literal `lg_config_k` from 4 through 21; it is retained in the type contract.
- HLL and Bitmap values cannot be cast to ordinary `binary()` or to each other.
- KLL and Theta declarations require the PySpark 4.1 profile; they are not baseline aggregate support.

## HLL

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `hll_sketch_agg(value, lg_config_k=12, where=None)` | HLL construction | `hll_sketch_agg(o.customer_id)` |
| `hll_union(left, right, allow_different_lg_config_k=False)` | `hll_union` | `hll_union(o.left_hll, o.right_hll)` |
| `hll_sketch_estimate(value)` | `hll_sketch_estimate` | `hll_sketch_estimate(o.customer_hll)` |

`hll_sketch_agg(...)` is a grouped aggregate and returns nullable opaque HLL state. `hll_union(...)` is row-local and
requires matching precision by default. Set `allow_different_lg_config_k=True` only for an intentional compatibility
case; it emits `SKETCH-W0802` because the target may reduce precision. `hll_sketch_estimate(...)` returns a nullable
Long approximation, not an exact distinct count.

## Bitmap

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `bitmap_construct_agg(value, where=None)` | Bitmap aggregate construction | `bitmap_construct_agg(o.feature_id)` |
| `bitmap_or_agg(value, where=None)` | `bitmap_or_agg` | `bitmap_or_agg(o.feature_bitmap)` |
| `bitmap_count(value)` | `bitmap_count` | `bitmap_count(o.feature_bitmap)` |
| `bitmap_bit_position(value)` | `bitmap_bit_position` | `bitmap_bit_position(o.feature_id)` |
| `bitmap_bucket_number(value)` | `bitmap_bucket_number` | `bitmap_bucket_number(o.feature_id)` |

Construction accepts Integer or Long positions. Construction and OR are grouped aggregates; count and position helpers
are row-local and return nullable Long values. A Bitmap consumer rejects an HLL, raw Binary, or a non-integral input.

## Operational boundary

Opaque state is compatible only with the appropriate Spark/profile implementation. It is not a portable Binary
interchange format. In streaming, scalar helpers preserve rows while aggregate helpers follow the ordinary grouped
watermark and output-mode rules. Structure does not own checkpoints, sinks, retention, or external sketch exchange.

See the [Sketches and Bitmaps reference](../reference/Sketches.ref.md) for usage and persistence guidance.
