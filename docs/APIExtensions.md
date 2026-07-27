# API Extensions

This page lists additions to PySpark APIs we implemented to facilitate writing data transform. They are contracts that do not 
correspond to a direct PySpark method or function.

For PySpark APIs, see [APICatalog.md](APICatalog.md). 

For Strucute APIs such as schemas, transforms, hooks, runtime invocation, generated-code lifecycle, etc. 
see Core APIs in [API.md](API.md) and [API.ref.md](reference/API.ref.md).

## Added PySpark Vocabulary

| API / Capability              | Status | Built on | Addition | Reference |
|-------------------------------| --- | --- | --- | --- |
| Schema fields in plain Python | implemented | Spark SQL types | `boolean`, `date`, `decimal`, `double`, `float`, `integer`, `long`, `map`, `string`, `struct`, `timestamp`, and field-form `array` declare PySpark-backed fields with Structure metadata | [Schema reference](reference/Schema.ref.md) |
| Enums/options                 | implemented | PySpark enum/string options | `AsOf`, `DecimalType`, `Join`, `JoinDedupe`, `JoinHint`, `JoinStrategy`, `OverlapPolicy`, `StreamingOutputMode`, `TiePolicy`, and `TimeWindow` make PySpark target options typed and discoverable | [APICatalog.md](APICatalog.md) |

## Added Relation Helpers

| API / Capability | Status | Built on | Addition | Reference |
| --- | --- | --- | --- | --- |
| Relation cardinality assertion | implemented | Spark-plan validation | `exactly_one` fails zero or multiple matches with stable `REL-E0701` diagnostics without driver collection | [APICatalog.md](APICatalog.md#relation-operations) |
| Relation integrity assertion | implemented | Spark-plan validation | `require_unique`, `require_all`, and `require_reference` express key, predicate, and parent-reference integrity checks with `REL-E0702`/`REL-E0703`/`REL-E0704` diagnostics | [APICatalog.md](APICatalog.md#relation-operations) |
| Priority row selection | implemented | Ordered grouping/window pattern | `select_first_qualified` selects one eligible row per declared business key and reports configured missing/tie failures as `REL-E0705` | [APICatalog.md](APICatalog.md#relation-operations) |
| Bounded parent hierarchy and fallbacks | scheduled | Iterative relation expansion pattern | Literal depth, missing-parent/cycle policy, and ordered fallback selection are admitted only as a narrow typed PySpark-plugin contract | Cohort traversal remains `@raw` until implemented |
| Bounded ordered `scan(...)` | scheduled | Ordered recurrence pattern | A separate typed recurrence contract will expose bounded PySpark state progression without general recursive DataFrame semantics | Sprint 26 |

## Added Selection Helpers

| API / Capability | Status | Built on | Addition | Reference |
| --- | --- | --- | --- | --- |
| Deterministic selected-row helpers | implemented | Ordered grouping/window patterns | `earliest_by`, `latest_by`, `dedupe_earliest_by`, and `dedupe_latest_by` encode common deterministic row-selection policies | [Aggregations API](api/Aggregations.api.md) |
| Temporal selected-row helpers | implemented | As-of join/window patterns | `temporal_one` and `as_of_one` make time direction, tolerance, and tie behavior explicit | [Joins API](api/Joins.api.md) |

## Other

Direct PySpark-compatible functions, joins, windows, aggregations, collection helpers, streaming transformations, set
operations, ordering, limits, and Column methods belong in [APICatalog.md](APICatalog.md), not here. Core Structure
APIs belong in [API.md](API.md) and [API.ref.md](reference/API.ref.md).
