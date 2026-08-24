# Relations API

These operations act on the current typed rowset rather than on one Column expression. They preserve Structure's
compiler-visible schema, cardinality, ordering, and streaming contracts.

The default transformation baseline is ordinary PySpark `>=3.5,<4.1`. Relation operations that are batch-only or have a
streaming schema-evolution gate are called out below.

## Aliases, Ordering, Bounds, And Sampling

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `relation_alias(...)` | DataFrame alias | `historical = relation_alias(customer, name="historical_customer")` |
| `order_by(...)` | `orderBy` | `latest = order_by(order.created_at.desc())` |
| `limit(...)` | `limit` | `latest = order_by(order.created_at.desc()).limit(1)` |
| `offset(...)` | `offset` | `page = order_by(order.created_at.asc()).offset(20)` |
| `sample(...)` | `sample` | `sample(0.25, seed=17)` |

**Details And Differences**

- `relation_alias(...)` creates a named typed occurrence of the current rowset or an unjoined relation for a self join.
  The name must be a unique, non-empty Python identifier within the step.
- `order_by(...)` requires at least one orderable expression. `limit(...)` and `offset(...)` require a preceding
  `order_by(...)` and non-negative integer literals; later row-shaping operations cannot silently preserve that order.
- `sample(...)` validates a literal fraction: `[0, 1]` without replacement and non-negative with replacement. A seed
  is required by default; `reproducible=False` explicitly opts into non-repeatable sampling.
- Ordering, bounds, and sampling are batch-oriented and are streaming materialization boundaries.

## Persistence And Lineage Boundaries

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `persist()` | `DataFrame.persist()` | `persist()` |
| `persist(storage_level=...)` | `DataFrame.persist(StorageLevel(...))` | `persist(storage_level=level)` |
| `cache()` | default `persist()` | `cache()` |
| `unpersist(blocking=False)` | `DataFrame.unpersist(blocking=...)` | `unpersist(blocking=True)` |
| `checkpoint(eager=True)` | `DataFrame.checkpoint(eager=...)` | `checkpoint()` |
| `local_checkpoint(eager=True)` | `DataFrame.localCheckpoint(eager=...)` | `local_checkpoint(eager=False)` |

`persist()` and `cache()` retain reusable data but do not truncate lazy logical lineage. `unpersist()` releases the
persisted data and also leaves lineage unchanged. `checkpoint()` writes through Spark's configured checkpoint
directory and truncates the logical plan. `local_checkpoint()` uses executor-local cached storage, so it is a
lineage boundary but not a reliable recovery boundary. Both checkpoint helpers are batch-only.

When a relation is repeatedly reused through self-joins or unions after its lineage has expanded, Structure emits
`PYSPARK-W2701`. Add `checkpoint()` or `local_checkpoint()` at the intended boundary; caching alone does not resolve
driver-side logical-plan analysis growth. See the [memory gotcha](../troubleshooting/memory/spark_driver_heap_oom.gotcha.md) and the developer [Memory specification](../dev/specifications/Memory.spec.md).

Interpret the remedy in three steps: **diminish** a multiplier with Structure's eligible projection-union fusion;
**bound** unchanged recursive reuse with `checkpoint()` or `local_checkpoint()`; or **remove** the recurrence by
rewriting the algorithm around a stable base relation when the business semantics permit it. Python assignment,
aliases, temporary views, `cache()`, and `persist()` do not bound logical lineage.

## Set Composition And Schema Evolution

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `union_all(...)` | `union` | `union_all(archived_orders)` |
| `union_by_name(...)` | `unionByName` | `union_by_name(archived_orders)` |
| `intersect(...)` | `intersect` | `intersect(reference_orders)` |
| `intersect_all(...)` | `intersectAll` | `intersect_all(reference_orders)` |
| `subtract(...)` | `subtract` | `subtract(reference_orders)` |
| `except_all(...)` | `exceptAll` | `except_all(reference_orders)` |

**Details And Differences**

- Exact-schema set operations require matching declared fields, physical names, types, and nullability. They preserve
  Spark duplicate semantics and make no ordering promise.
- `union_by_name(..., allow_missing_columns=True)` is the batch evolution form. Nullable missing fields are filled with
  null; `defaults={"field.path": literal}` supplies typed literals for missing non-nullable fields.
- Defaults use canonical Structure field paths and support nested struct fields, complete struct defaults, and physical
  alias preservation. Non-literal, unknown, or type-incompatible defaults are rejected.
- Implicit evolution inside arrays and maps is rejected. Missing-column union for streaming relations remains
  streaming-ineligible until target-specific restart evidence is complete.
- Set operations must occur before joins, generators, aggregation, or selected-row operations in the same step.

## Relation Assertions

| Structure API | Contract | Example |
| --- | --- | --- |
| `exactly_one(...)` | Fail unless a relation has exactly one row | `exactly_one(customer)` |
| `require_unique(...)` | Fail on duplicate key tuples | `require_unique(order.customer_id)` |
| `require_all(...)` | Fail when any row violates a predicate | `require_all(order.total >= 0)` |
| `require_reference(...)` | Missing reference row | `require_reference(order.customer_id, customers)` |
| `require_parent_hierarchy(...)` | Validate parent links | `require_parent_hierarchy(id, parent_id, max_depth=20)` |

**Details And Differences**

- Assertions preserve the current typed rowset on success and fail at Spark evaluation without driver collection or
  implicit filtering.
- `require_reference(...)` allows null values by default; use `nulls="reject"` to treat them as violations.
- Parent validation reports missing parents, cycles, depth overruns, and invalid child ordering through `REL-E0706`.
- These assertions are batch-only until a streaming validation contract exists.

## Hierarchy And Priority Selection

| Structure API | Contract | Example |
| --- | --- | --- |
| `hierarchy_closure(...)` | Emit node/ancestor/depth rows | `hierarchy_closure(id, parent_id, max_depth=20)` |
| `hierarchy_fallbacks(...)` | Expand fallback paths | `hierarchy_fallbacks(source_id, path, parents)` |
| `select_first_qualified(...)` | Select eligible row per key | `select_first_qualified(customer_id, where=active)` |

**Details And Differences**

- Hierarchy expansion is finite and uses typed self-join plans; it does not use recursion, driver collection, or a
  Python UDF. `max_depth` must be a positive literal.
- `select_first_qualified(...)` requires declared business keys, an eligibility predicate, and an explicit priority
  order. `missing="error"` fails when no row qualifies; `ties="error"` is the supported tie policy.
- Hierarchy expansion and priority selection are batch-only and preserve explicit cardinality and tie behavior.

## Bounded Ordered Scan

`scan(...)` expresses a batch-only, bounded recurrence over a partitioned and ordered timeline:

```python
running = scan(
    initial=Balance(total=0),
    partition_by=entry.account_id,
    order_by=entry.posted_at,
    max_rows=1000,
    step=lambda state, row: Balance(total=state.total + row.amount),
)
```

The initial state must fully populate its Schema. The transition callback receives the previous typed state and current
row, and must return the same state Schema. A positive `max_rows` bounds the inspected history per output row; duplicate
ordering keys fail under the current `ties="error"` policy.

See [Ordered Timeline Scan](../dev/specifications/OrderedTimelineScan.spec.md) for the complete state and recurrence rules.
