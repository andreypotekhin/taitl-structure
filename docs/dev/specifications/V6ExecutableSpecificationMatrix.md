# V6 Executable Specification Matrix

This matrix records the executable evidence for v6 features. A row is complete only when it names the behavior,
specification owner, and the checked test files that prove source capture, generated rendering, online execution,
traceability, compatibility, or live behavior as applicable.

Live Spark tests may be skipped in a local workspace without a live target. Skipped live lanes are evidence of test
presence, not evidence that the target passed.

| Capability | Specification | Executable evidence |
| --- | --- | --- |
| Raw-hook inventory and gaps register | `docs/dev/specifications/V6PySparkApiLedger.md` | `tests/specifications/v6-api-ledger/test_v6_raw_hook_inventory.py` |
| Security/Search migration prerequisites | `docs/dev/specifications/V6PySparkApiLedger.md` | `tests/specifications/v6-api-ledger/test_v6_example_migration_prerequisites.py`, `tests/integration/pyspark/security/test_security.py`, `tests/integration/pyspark/search/test_search.py` |
| Partitioned `window_max` | `docs/dev/design/V6PySparkApiClosure.md` | `tests/specifications/v6-api-ledger/test_v6_window_max_partitioning.py` |
| Ordered `collect_list` | `docs/dev/design/V6PySparkApiClosure.md` | `tests/specifications/v6-api-ledger/test_v6_ordered_collect_list.py` |
| `exactly_one(relation)` | `docs/dev/specifications/TypedRelationOperations.md` | `tests/specifications/v6-api-ledger/test_v6_exactly_one_relation.py` |
| `posexplode_struct(...)` typed generator | `docs/dev/specifications/TypedRelationOperations.md` | `tests/specifications/v6-api-ledger/test_v6_posexplode_struct.py` |
| Exact-schema relation set composition | `docs/dev/specifications/TypedRelationOperations.md` | `tests/specifications/v6-api-ledger/test_v6_relation_union.py` |
| Named relation aliases | `docs/dev/specifications/TypedRelationOperations.md` | `tests/specifications/v6-api-ledger/test_v6_relation_alias.py` |
| Relation ordering and bounds | `docs/dev/specifications/TypedRelationOperations.md` | `tests/specifications/v6-api-ledger/test_v6_relation_ordering.py` |
| Relation assertions and reference checks | `docs/dev/specifications/TypedRelationOperations.md` | `tests/specifications/v6-api-ledger/test_v6_relation_assertions.py` |
| Parent hierarchy validation, closure, and fallbacks | `docs/dev/specifications/TypedRelationOperations.md` | `tests/specifications/v6-api-ledger/test_v6_relation_hierarchy.py` |
| First-qualified priority selection | `docs/dev/specifications/TypedRelationOperations.md` | `tests/specifications/v6-api-ledger/test_v6_priority_selection.py` |
| Bounded ordered `scan(...)` | `docs/dev/planning/P07182601.V6-timeline-scan-recurrence.plan.md` | `tests/specifications/symbolic-execution/test_ordered_timeline_scan.py`, `tests/integration/pyspark/v6/test_ordered_timeline_scan.py`, `tests/integration/pyspark/v6/test_school_sequence_series.py` |
| PySpark capability catalog consistency | `docs/APICatalog.md` | `tests/specifications/compatibility/test_pyspark_transformation_coverage.py`, `tests/specifications/backend-capabilities/test_backend_capabilities.py` |

## Release Rule

Before a v6 capability is marked implemented in `docs/APICatalog.md` or `docs/dev/Gaps.md`, add it to this matrix with
at least one executable specification test and every relevant generated, online, traceability, compatibility, or live
test file. Rows may reference skipped live tests, but release notes must say they were skipped unless the live lane
actually ran.
