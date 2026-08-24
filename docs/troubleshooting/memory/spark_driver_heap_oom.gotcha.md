# Spark driver heap exhaustion from reused lazy lineage

### Problem (integration): Reused lazy lineage grows until driver analysis exhausts the heap

When: A PySpark program repeatedly aliases, joins, projects, or unions DataFrames that still reference the complete
lineage built by earlier iterations.

Error: A small input can spend little time executing but a long time constructing or analyzing the logical plan. The
driver may report `java.lang.OutOfMemoryError: Java heap space` during `count()` or another action. Secondary RPC,
Netty, or scheduler errors can appear after the Spark JVM has already run out of memory.

Cause: Spark DataFrames are lazy logical-plan handles. A Python variable, alias, cache, persist, or ordinary temporary
view does not necessarily materialize the data or shorten the logical lineage. Reusing an already-expanded DataFrame on
both sides of a self-join duplicates that history again.

The developer-facing root-cause analysis, measurements, and design decisions are in the
[Memory specification](../../dev/specifications/Memory.spec.md). The implementation work is tracked in the
[lineage materialization plan](../../dev/planning/P08232601.PySpark-lineage-materialization-and-diagnostics.plan.md).

## Reproduce

The self-contained example is [`spark_driver_heap_oom.py`](spark_driver_heap_oom.py). It uses only PySpark and two input
rows, so the data volume is deliberately irrelevant. Run it from the repository root in the PySpark 3.5 integration
image:

    docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm --entrypoint bash -e STRUCTURE_SPARK_DRIVER_MEMORY=1g structure-integration-pyspark35 -lc "python /workspace/docs/troubleshooting/memory/spark_driver_heap_oom.py --rounds 7"

The default `union` mode builds the problematic reverse DataFrame branch. The `explode` mode emits both directions in
one projection and demonstrates a smaller, but still growing, plan:

    docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm --entrypoint bash -e STRUCTURE_SPARK_DRIVER_MEMORY=1g structure-integration-pyspark35 -lc "python /workspace/docs/troubleshooting/memory/spark_driver_heap_oom.py --rounds 7 --directions explode"

For Spark 4.0, run from writable `/tmp` because Spark's artifact manager cannot create its temporary directory under the
read-only `/workspace` mount:

    docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm --workdir /tmp --entrypoint bash structure-integration-pyspark40 -lc "python /workspace/docs/troubleshooting/memory/spark_driver_heap_oom.py --rounds 4 --directions explode"

## Fix

The remedy has three distinct meanings:

- **Diminish:** An equivalent projection-union rewrite can remove one repeated branch and delay failure, but recursive
  reuse can remain exponential.
- **Bound:** `checkpoint()` or `local_checkpoint()` creates a lineage boundary at the selected point. The regular
  checkpoint uses configured checkpoint storage; local checkpointing uses executor-local storage and is not a reliable
  recovery boundary.
- **Remove:** Restructure the algorithm so each round depends on a small stable base relation or an equivalent bounded
  reduction. This requires preserving the application's business semantics.

`cache()` and `persist()` can improve physical reuse, but they do not solve driver-side logical-plan growth. Python
assignment, aliases, and temporary views are not substitutes for checkpointing. Increasing the driver heap can postpone
the failure but does not change the plan shape.

For the isolated example, checkpoint each round when the growing relation must be reused:

    docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm --entrypoint bash -e STRUCTURE_SPARK_DRIVER_MEMORY=1g structure-integration-pyspark35 -lc "python /workspace/docs/troubleshooting/memory/spark_driver_heap_oom.py --rounds 8 --checkpoint-every 1"

For Search-like reciprocal processing, keep retrieval, scoring, and reranking relations at bounded grains; emit forward
and reverse rows in one typed expansion when that preserves the required row semantics; and checkpoint before reusing an
expanded candidate relation when the boundary is acceptable. Replace a reciprocal self-join with a keyed reduction only
when its uniqueness and reciprocal-semantics assumptions are proven.

Structure emits `PYSPARK-W2701` when its compiler sees the repeated-reuse shape. See the
[diagnostics catalog](../../Diagnostics.md) for the warning and configuration option.
