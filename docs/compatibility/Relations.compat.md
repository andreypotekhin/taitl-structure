# Relations Compatibility

This is the compatibility companion to the [API reference](../api/Relations.api.md). It records Structure contracts alongside the corresponding PySpark API forms and examples for read-through. The shared baseline is the public PySpark 3.5.x/4.0.x intersection; Connect is claimed only where runtime evidence is recorded.

## Writer Partition Transforms

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| — | `years` | — | yes | yes | Status: `caller-owned-guided`. Output transform for Date/Timestamp values; only valid in `DataFrameWriterV2.partitionedBy`. Migration: Use native PySpark writer APIs. |
| — | `months` | — | yes | yes | Status: `caller-owned-guided`. Output transform for Date/Timestamp values; only valid in `DataFrameWriterV2.partitionedBy`. Migration: Use native PySpark writer APIs. |
| — | `days` | — | yes | yes | Status: `caller-owned-guided`. Output transform for Date/Timestamp values; only valid in `DataFrameWriterV2.partitionedBy`. Migration: Use native PySpark writer APIs. |
| — | `hours` | — | yes | yes | Status: `caller-owned-guided`. Output transform for Timestamp values; only valid in `DataFrameWriterV2.partitionedBy`. Migration: Use native PySpark writer APIs. |
| — | `bucket` | — | yes | yes | Status: `caller-owned-guided`. Hash-bucket output transform for any input type; only valid in `DataFrameWriterV2.partitionedBy`. Migration: Use native PySpark writer APIs. |

## Function-index dispositions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| — | `broadcast` | — | yes | yes | Status: `caller-owned-guided`. Relation-level broadcast choice is expressed as `lookup_join(..., hint="broadcast")`, not as a row expression. Migration: Attach the broadcast hint to the Structure join; retain native PySpark for standalone relation-marker use. |

## Relation, writer, and validation helpers

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `relation_alias(...)` | DataFrame alias | `historical = relation_alias(customer, name="historical_customer")` | yes | yes | `relation_alias(...)` creates a named typed occurrence of the current rowset or an unjoined relation for a self join. The name must be a unique, non-empty Python identifier within the step. |
| `order_by(...)` | `orderBy` | `latest = order_by(order.created_at.desc())` | yes | yes | `order_by(...)` requires at least one orderable expression. `limit(...)` and `offset(...)` require a preceding `order_by(...)` and non-negative integer literals; later row-shaping operations cannot silently preserve that order. `order_by(...)` and `repartition_by_range(...)` accept raw orderable expressions or any of the six typed ordering descriptors. Invalid descriptor operands fail during Structure compilation. `repartition_by_range(count, *orderings)` redistributes the current rowset into `count` range partitions using one or more typed order expressions. `count` must be a positive integer literal. The operation preserves rows and schema, but it does not promise the final materialized row order; use `order_by(...)` when output order matters. |
| `repartition(...)` | `DataFrame.repartition` | `by_customer = repartition(8, order.customer_id)` | yes | yes | `repartition(count, *keys)` hash-shuffles the current rowset to a positive partition count and optional key expressions; `repartition(*keys)` uses Spark's configured shuffle partition count. A leading integer always means the partition count, matching PySpark. Use an explicit literal expression (for example, `literal(8)`) to hash on a constant integer key. The operation preserves rows and schema, but makes no order or stable partition-identity promise. Streaming use is compatible with `STREAM-W0804`, a throughput advisory. Range repartitioning remains batch-only. |
| `repartition_by_range(...)` | `repartitionByRange` | `partitioned = repartition_by_range(8, order.customer_id)` | yes | yes | `order_by(...)` and `repartition_by_range(...)` accept raw orderable expressions or any of the six typed ordering descriptors. Invalid descriptor operands fail during Structure compilation. `repartition_by_range(count, *orderings)` redistributes the current rowset into `count` range partitions using one or more typed order expressions. `count` must be a positive integer literal. The operation preserves rows and schema, but it does not promise the final materialized row order; use `order_by(...)` when output order matters. |
| `coalesce(...)` | `DataFrame.coalesce` | `fewer_partitions = coalesce(partitions=4)` | yes | yes | `coalesce(*, partitions=count)` reduces the current rowset to a positive number of partitions without changing rows or schema. The keyword-only argument keeps this relation operation distinct from scalar `coalesce(value, fallback, *values)`, which requires at least two values. Partition layout and output order are not stable contracts. On streaming relations, Structure emits `STREAM-W0803`: treat the count as a throughput-tuning hint, not a stable partition identity or ordering guarantee. |
| `limit(...)` | `limit` | `order_by(order.created_at.desc()); limit(1)` | yes | yes | `order_by(...)` requires at least one orderable expression. `limit(...)` and `offset(...)` require a preceding `order_by(...)` and non-negative integer literals; later row-shaping operations cannot silently preserve that order. Spark can eliminate guards together with unused work, including a constant-false filter or `limit(0)`. A successful partial or empty result does not certify the entire input. Do not use these assertions as an unconditional audit. |
| `offset(...)` | `offset` | `order_by(order.created_at.asc()); offset(20)` | yes | yes | `order_by(...)` requires at least one orderable expression. `limit(...)` and `offset(...)` require a preceding `order_by(...)` and non-negative integer literals; later row-shaping operations cannot silently preserve that order. |
| `sample(...)` | `sample` | `sample(0.25, seed=17)` | yes | yes | `sample(...)` validates a literal fraction: `[0, 1]` without replacement and non-negative with replacement. A seed is required by default; `reproducible=False` explicitly opts into non-repeatable sampling. |
| `persist(...)` | `DataFrame.persist` | `persist()` | yes | yes | Persists the current relation using Spark's default storage level; pass `storage_level=` to select another PySpark `StorageLevel`. |
| `persist(...)` | `DataFrame.persist` | `persist(storage_level=level)` | yes | yes | Persists the current relation at the requested Spark `StorageLevel` and returns the same typed rowset. |
| `cache(...)` | `persist` | `cache()` | yes | yes | Caches the current relation using Spark's default storage level; use `persist(storage_level=...)` when a different level is required. |
| `unpersist(...)` | `DataFrame.unpersist` | `unpersist(blocking=True)` | yes | yes | Removes the current relation's persisted blocks; `blocking=True` waits for Spark to finish removing them. |
| `checkpoint(...)` | `DataFrame.checkpoint` | `checkpoint()` | yes | yes | Writes a reliable checkpoint and truncates the upstream logical plan; Spark's checkpoint directory must be configured. Live batch evidence covers Spark Connect 4.0 and 4.1, including deep input plans, preserved aliases, and temporary-view cleanup; the full Connect 4.1 suite passes. Connect 3.5 remains gated. |
| `local_checkpoint(...)` | `DataFrame.localCheckpoint` | `local_checkpoint(eager=False)` | yes | yes | Truncates the upstream plan using executor-local storage; unlike `checkpoint(...)`, the checkpoint is not reliable across executor loss. |
| `union_all(...)` | `union` | `union_all(archived_orders)` | yes | yes | Appends rows positionally and preserves duplicates; input fields must have compatible ordered types. Use `union_by_name(...)` for name-based alignment. |
| `union_by_name(...)` | `unionByName` | `union_by_name(archived_orders)` | yes | yes | `union_by_name(..., allow_missing_columns=True)` is the batch evolution form. Nullable missing fields are filled with null; `defaults={"field.path": literal}` supplies typed literals for missing non-nullable fields. |
| `intersect(...)` | `intersect` | `intersect(reference_orders)` | yes | yes | Returns distinct rows present in both rowsets; schemas must be positionally compatible. |
| `intersect_all(...)` | `intersectAll` | `intersect_all(reference_orders)` | yes | yes | Returns common rows with duplicate multiplicity preserved up to the lower input count. |
| `subtract(...)` | `subtract` | `subtract(reference_orders)` | yes | yes | Returns distinct left-side rows absent from the right rowset. |
| `except_all(...)` | `exceptAll` | `except_all(reference_orders)` | yes | yes | Removes right-side rows while preserving the remaining duplicate multiplicity. |
| `exactly_one(...)` | Relation cardinality assertion | `exactly_one(customer)` | yes | yes | Requires exactly one row. Relation assertions preserve valid rows and multiplicities, build lazy aggregate guards, and run no validation job during construction; an action evaluates retained guards. Assertions are batch-only. |
| `require_unique(...)` | Duplicate-key assertion | `require_unique(order.customer_id)` | yes | yes | Fails when key tuples are duplicated. Relation assertions preserve valid rows and multiplicities, build lazy aggregate guards, and run no validation job during construction; an action evaluates retained guards. Assertions are batch-only. |
| `require_all(...)` | Predicate assertion | `require_all(order.total >= 0)` | yes | yes | Fails when any row violates the predicate. Relation assertions preserve valid rows and multiplicities, build lazy aggregate guards, and run no validation job during construction; an action evaluates retained guards. Assertions are batch-only. |
| `require_reference(...)` | Reference-integrity assertion | `require_reference(order.customer_id, customers, reference_key=customers.id)` | yes | yes | Fails for missing reference values; nulls are allowed by default, while `nulls="reject"` treats them as violations. Assertions are batch-only. |
| `require_parent_hierarchy(...)` | Parent-link assertion | `require_parent_hierarchy(id, parent=parent_id, order_by=priority, max_depth=20)` | yes | yes | Validates missing parents, cycles, depth overruns, and child ordering; violations use `REL-E0706`. Assertions are batch-only. |
| `hierarchy_closure(...)` | Typed hierarchy expansion | `hierarchy_closure(id, parent_id, max_depth=20)` | yes | yes | Emits node/ancestor/depth rows using finite typed self-joins, not recursion, driver collection, or a Python UDF. `max_depth` must be a positive literal; batch-only. |
| `hierarchy_fallbacks(...)` | Typed fallback-path expansion | `hierarchy_fallbacks(source_id, path, parents)` | yes | yes | Expands fallback paths using typed relational operations; hierarchy expansion is batch-only and preserves explicit cardinality. |
| `select_first_qualified(...)` | Priority selection | `select_first_qualified(customer_id, where=active)` | yes | yes | Requires declared business keys, an eligibility predicate, and explicit priority order. `missing="error"` fails when no row qualifies; `ties="error"` is the supported tie policy. Batch-only. |

## Target-gated and boundary items

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| — | Writer partition transforms | — | yes | yes | Status: `caller-owned-guided`. Output file layout remains separate from relation distribution and caller-owned. Migration: Use native PySpark writer partition transforms. |
## Unsupported

These PySpark functions or behaviors have no equivalent admitted Structure contract. Use the stated caller-owned or typed alternative.

| PySpark parity | Details |
| --- | --- |
| `input_file_block_length` | Status: `caller-owned-guided`. File split metadata depends on the physical input plan. Migration: Keep file metadata access in native PySpark at a physical-input boundary. |
| `input_file_block_start` | Status: `caller-owned-guided`. File split metadata depends on the physical input plan. Migration: Keep file metadata access in native PySpark at a physical-input boundary. |
| `input_file_name` | Status: `caller-owned-guided`. Input path is physical-source metadata and is not portable across execution plans. Migration: Keep input-path access in native PySpark at a physical-input boundary. |
| `monotonically_increasing_id` | Status: `caller-owned-guided`. Generated identifiers depend on physical partitioning and are not stable across plans. Migration: Generate IDs in native PySpark only when physical-plan dependence is acceptable. |
| `spark_partition_id` | Status: `caller-owned-guided`. Partition identity exposes the physical execution plan. Migration: Keep partition inspection in native PySpark at an explicit physical-plan boundary. |
