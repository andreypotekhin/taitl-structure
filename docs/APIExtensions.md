# API Extensions

This page lists additions to PySpark APIs added to facilitate writing data transforms. They are contracts that do not correspond to a direct PySpark method or function.

For PySpark APIs, see [APICatalog.md](APICatalog.md). For detailed whole-rowset contracts, see the
[Relations API](api/Relations.api.md). For Structure APIs such as schemas, transforms, and hooks, see [API.md](API.md).

## Added PySpark Vocabulary

| Capability              | Status | Built on | Addition | Reference |
|-------------------------------| --- | --- | --- | --- |
| Schema fields in plain Python | implemented | Spark SQL types | `boolean`, `date`, `decimal`, `double`, `float`, `integer`, `long`, `map`, `string`, `struct`, `timestamp`, and field-form `array` declare fields lowering to PySpark schemas. | [Schema reference](reference/Schema.ref.md) |
| Options                       | implemented | PySpark string options | Join, as-of, overlap, and tie options accept PySpark-style string literals and validate them before compilation; constants remain available as aliases. | [APICatalog.md](APICatalog.md) |

## Added Relation Helpers

| Capability | Status | Built on | Addition | Reference |
| --- | --- | --- | --- | --- |
| Relation cardinality assertion | implemented | Spark-plan validation | `exactly_one` fails zero or multiple matches with stable `REL-E0701` diagnostics without driver collection | [Relations API](api/Relations.api.md) |
| Relation integrity assertion | implemented | Spark-plan validation | `require_unique`, `require_all`, and `require_reference` express typed integrity checks | [Relations API](api/Relations.api.md) |
| Parent hierarchy validation | implemented | Finite self-join validation pattern | `require_parent_hierarchy` checks bounded catalogs and reports `REL-E0706` | [Relations API](api/Relations.api.md) |
| Priority row selection | implemented | Ordered grouping/window pattern | `select_first_qualified` selects one eligible row per declared business key and reports `REL-E0705` | [Relations API](api/Relations.api.md) |
| Parent hierarchy closure | implemented | Iterative relation expansion pattern | `hierarchy_closure` emits typed `(node, ancestor, depth)` closure rows | [Relations API](api/Relations.api.md) |
| Bounded parent hierarchy fallbacks | implemented | Iterative relation expansion pattern | `hierarchy_fallbacks` emits deterministic fallback rows from a bounded path | [Relations API](api/Relations.api.md) |
| Relation sampling | implemented | Spark `DataFrame.sample` | `sample(fraction, seed=...)` records reproducible batch sampling | [Relations API](api/Relations.api.md) |
| Missing-column union | implemented/design-gated | Spark `DataFrame.unionByName` | Batch nullable/defaulted and nested-struct evolution is supported; array/map and streaming evolution remain gated | [Relations API](api/Relations.api.md) |
| Bounded ordered `scan(...)` | implemented | Ordered recurrence pattern | Batch-only typed state progression over a bounded ordered timeline | [Relations API](api/Relations.api.md) |

## Added Selection Helpers

| Capability | Status | Built on | Addition | Reference |
| --- | --- | --- | --- | --- |
| Deterministic selected-row helpers | implemented | Ordered grouping/window patterns | `earliest_by`, `latest_by`, `dedupe_earliest_by`, and `dedupe_latest_by` encode common deterministic row-selection policies | [Aggregations API](api/Aggregations.api.md) |
| Temporal selected-row helpers | implemented | As-of join/window patterns | `temporal_one` and `as_of_one` make time direction, tolerance, and tie behavior explicit | [Joins API](api/Joins.api.md) |

## Other

Direct PySpark-compatible functions, joins, windows, aggregations, collection helpers, streaming transformations, set
operations, ordering, limits, and Column methods belong in [APICatalog.md](APICatalog.md), not here. Core Structure
APIs belong in [API.md](API.md) and [API.ref.md](reference/API.ref.md).
