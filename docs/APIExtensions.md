# API Extensions

This page lists additions to PySpark APIs added to facilitate writing data transforms. They are contracts that do not correspond to a direct PySpark method or function.

For PySpark APIs, see [APICatalog.md](APICatalog.md). For Structure APIs such as schemas, transforms, hooks etc. see Core APIs in [API.md](API.md).

## Added PySpark Vocabulary

| Capability              | Status | Built on | Addition | Reference |
|-------------------------------| --- | --- | --- | --- |
| Schema fields in plain Python | implemented | Spark SQL types | `boolean`, `date`, `decimal`, `double`, `float`, `integer`, `long`, `map`, `string`, `struct`, `timestamp`, and field-form `array` declare fields lowering to PySpark schemas. | [Schema reference](reference/Schema.ref.md) |
| Options                       | implemented | PySpark string options | Join, as-of, overlap, and tie options accept PySpark-style string literals and validate them before compilation; constants remain available as aliases. | [APICatalog.md](APICatalog.md) |

## Added Relation Helpers

| Capability | Status | Built on | Addition | Reference |
| --- | --- | --- | --- | --- |
| Relation cardinality assertion | implemented | Spark-plan validation | `exactly_one` fails zero or multiple matches with stable `REL-E0701` diagnostics without driver collection | [APICatalog.md](APICatalog.md#relation-operations) |
| Relation integrity assertion | implemented | Spark-plan validation | `require_unique`, `require_all`, and `require_reference` express key, predicate, and parent-reference integrity checks with `REL-E0702`/`REL-E0703`/`REL-E0704` diagnostics | [APICatalog.md](APICatalog.md#relation-operations) |
| Parent hierarchy validation | implemented | Finite self-join validation pattern | `require_parent_hierarchy` checks bounded parent catalogs for missing parents, cycles, depth overruns, and child ordering with `REL-E0706` diagnostics | [APICatalog.md](APICatalog.md#relation-operations) |
| Priority row selection | implemented | Ordered grouping/window pattern | `select_first_qualified` selects one eligible row per declared business key and reports configured missing/tie failures as `REL-E0705` | [APICatalog.md](APICatalog.md#relation-operations) |
| Parent hierarchy closure | implemented | Iterative relation expansion pattern | `hierarchy_closure` emits typed `(node, ancestor, depth)` closure rows from a bounded parent catalog without driver collection | [APICatalog.md](APICatalog.md#relation-operations) |
| Bounded parent hierarchy fallbacks | implemented | Iterative relation expansion pattern | `hierarchy_fallbacks` emits deterministic fallback rows from a declared band-id path and unjoined parent catalog without driver collection | [APICatalog.md](APICatalog.md#relation-operations) |
| Relation sampling | implemented | Spark `DataFrame.sample` | `sample(fraction, seed=...)` records reproducible batch sampling; `reproducible=False` is required for unseeded sampling | [APICatalog.md](APICatalog.md#relation-operations) |
| Missing-column union | implemented/design-gated | Spark `DataFrame.unionByName` | `union_by_name(relation, allow_missing_columns=True)` supports nullable fills, typed scalar defaults, canonical nested struct paths, aliases, and explicit struct defaults for batch relations; array/map element evolution and streaming missing-column union remain gated | [APICatalog.md](APICatalog.md#relation-operations) |
| Bounded ordered `scan(...)` | implemented | Ordered recurrence pattern | Batch-only typed state progression over a caller-supplied, partitioned, ordered timeline without general recursive DataFrame semantics | [Ordered Timeline Scan](dev/specifications/OrderedTimelineScan.md) |

## Added Selection Helpers

| Capability | Status | Built on | Addition | Reference |
| --- | --- | --- | --- | --- |
| Deterministic selected-row helpers | implemented | Ordered grouping/window patterns | `earliest_by`, `latest_by`, `dedupe_earliest_by`, and `dedupe_latest_by` encode common deterministic row-selection policies | [Aggregations API](api/Aggregations.api.md) |
| Temporal selected-row helpers | implemented | As-of join/window patterns | `temporal_one` and `as_of_one` make time direction, tolerance, and tie behavior explicit | [Joins API](api/Joins.api.md) |

## Other

Direct PySpark-compatible functions, joins, windows, aggregations, collection helpers, streaming transformations, set
operations, ordering, limits, and Column methods belong in [APICatalog.md](APICatalog.md), not here. Core Structure
APIs belong in [API.md](API.md) and [API.ref.md](reference/API.ref.md).
