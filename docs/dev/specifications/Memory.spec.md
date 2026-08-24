# PySpark Driver Memory and Logical-Lineage Boundaries

## Purpose

This specification records the reproducible PySpark driver-heap exhaustion case that motivated Structure's explicit
materialization helpers, projection-union optimization, and `PYSPARK-W2701` diagnostic. It is the developer-facing
source of truth for the measurements, root-cause model, implementation decisions, and acceptance evidence.

End-user symptoms and remedies belong in the [memory gotcha](../../troubleshooting/memory/spark_driver_heap_oom.gotcha.md).
The implementation work is tracked in the [lineage materialization and diagnostics plan](../planning/P08232601.PySpark-lineage-materialization-and-diagnostics.plan.md).

## Scope

This specification covers driver-side memory consumed while Spark analyzes a growing lazy DataFrame logical plan. It
does not describe executor memory, shuffle spill, RDD storage hygiene, or application-wide heap tuning. It also does
not change Search reciprocal semantics or prescribe an automatic checkpoint policy.

The committed feature scope is:

- compiler-visible `persist()`, `cache()`, `unpersist()`, `checkpoint()`, and `local_checkpoint()` operations;
- generated and online execution parity for supported ordinary PySpark profiles;
- deterministic same-source projection-union fusion;
- default-on structural warning `PYSPARK-W2701` with project and transform-level configuration;
- explain output that distinguishes a reduced multiplier from a true lineage boundary.

## Terminology

**Lazy lineage** is Spark's logical description of how a DataFrame is derived. A Python variable, alias, temporary view,
cache, or persist may give the user a new handle or improve physical reuse without shortening that logical description.

**True boundary** means `checkpoint()` or `local_checkpoint()` in this specification. These operations replace the
current logical dependency with a checkpointed relation. `checkpoint()` uses the configured checkpoint directory;
`local_checkpoint()` uses executor-local storage and is not a reliable recovery boundary.

**Diminish** means lowering a lineage multiplier while leaving recursive growth possible. **Bound** means truncating the
lineage at an explicit checkpoint boundary. **Remove** means restructuring the algorithm so expanded lineage is not fed
back into the next round.

## Reproducible Case

The self-sufficient fixture is
`docs/troubleshooting/memory/spark_driver_heap_oom.py`. It imports only PySpark and starts with two rows. Each round:

1. aliases the current DataFrame twice;
2. self-joins the aliases;
3. selects a canonical pair;
4. creates the reversed pair; and
5. unions the canonical and reversed DataFrames.

The essential shape is:

    left = current.alias(f"left_{round_number}")
    right = current.alias(f"right_{round_number}")
    joined = left.join(right, join_condition)
    canonical = joined.where(predicate).select(left_id, right_id)
    reverse = canonical.select("right_id", "left_id")
    current = canonical.unionByName(reverse)

Run the baseline from the repository root in the PySpark 3.5 integration image:

    docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm --entrypoint bash -e STRUCTURE_SPARK_DRIVER_MEMORY=1g structure-integration-pyspark35 -lc "python /workspace/docs/troubleshooting/memory/spark_driver_heap_oom.py --rounds 7"

The expected failure is `java.lang.OutOfMemoryError: Java heap space` during driver-side analysis or the final action,
even though the input contains only two rows. The end-user reproduction and shorter commands are in the gotcha.

## Root Cause

The self-join references the incoming logical plan twice. The reverse projection and final union then reference the
expanded canonical plan again. If `L_n` is the logical-lineage size after round `n`, the observed shape is approximately

    L_(n+1) = 4.5 L_n + fixed operation text.

The factor is not an infinite allocator or a leak that ignores `-Xmx`; it is finite exponential graph duplication for a
finite requested plan. As rounds or reused branches increase, the required driver heap becomes unbounded relative to
the requested graph. Catalyst analyzer work such as relation deduplication, reference traversal, and attribute-set
construction recursively visits the duplicated graph before meaningful worker computation begins.

`cache()` and `persist()` can reduce recomputation cost, but they do not truncate the logical lineage. A Python alias,
assignment, or ordinary temporary view likewise does not provide a driver-analysis boundary.

## Measurements

The following measurements were collected with PySpark 3.5 and a 1 GiB driver heap:

| Round | Baseline union plan characters | Fused explode plan characters |
| ---: | ---: | ---: |
| 1 | 1,211 | 760 |
| 2 | 6,987 | 2,596 |
| 3 | 35,617 | 7,108 |
| 4 | 172,213 | 17,841 |
| 5 | 806,405 | 42,676 |
| 6 | 3,698,529 | 99,066 |
| 7 | 16,680,913 | 225,286 |
| 10 | not reached | 2,449,919 |
| 12 | not reached | 11,525,294 |
| 13 | not reached | 24,772,674 |

The baseline reaches the documented heap failure at round seven. The fused shape reduces the observed recurrence to
about 2.2x, is about 74 times smaller at round seven, completes twelve rounds, and fails at round thirteen. This proves
that fusion diminishes one multiplier but does not bound the remaining self-join recurrence.

Checkpointing every round keeps plan text between 46 and 48 characters through eight rounds and completes with
`rows=0` under the same 1 GiB setting. A temporary view displayed a 59-character unresolved plan but still failed while
creating round eight; the nested Catalyst lineage remained behind the relation name.

PySpark 4.0 reproduced the fused shape from writable `/tmp` with plan sizes `754`, `2,600`, `7,132`, and `17,992` for
four rounds and completed with `rows=0`. Running from the read-only `/workspace` first exposed Spark 4.0's artifact
directory requirement. Spark Connect 3.5 smoke coverage passed independently, but materialization helpers remain
unsupported for Connect because no helper-method capability claim has been accepted.

## Design Decisions

### Explicit helpers are compiler-visible

The helpers are captured in `OperationPlan`, mapped to execution recipes, rendered with public PySpark DataFrame
methods, and applied by online execution in source order. This preserves the user's boundary position and makes
generated and online behavior testable as one contract.

`cache()` is the default-storage-level form of `persist()`. `@step(cache=...)` remains supported. `unpersist()` releases
persistence but does not change lineage. `checkpoint()` and `local_checkpoint()` are explicit batch-only lineage
boundaries; no automatic checkpoint is inserted.

### Projection-union fusion is a safe reduction, not materialization

The compiler may fuse a private deterministic row-preserving projection branch when it is unioned back into its sole
source with identical schemas and no filters, joins, aggregates, windows, generators, hooks, assertions, validation
side effects, missing-column defaults, nondeterminism, or materialization operations. The lowered form emits typed
forward and projected structs through one `explode_struct` operation and preserves duplicate row multiplicity.

The optimizer records `projection-union fusion: <projection> + <merge>` in traceability. It must never suppress
`PYSPARK-W2701` merely because fusion applied: fusion is **Diminish**, not **Bound**.

### Warning and explain contract

`PYSPARK-W2701` is default-on and structurally analyzes captured operations without starting Spark or inspecting private
Spark handles. It is deduplicated per step, reset by explicit checkpoint/local-checkpoint operations, and configurable
through `warn_on_lineage_growth` at project and transform scope.

When lowering confirms fusion, the warning reports both facts: the multiplier was diminished and the remaining self-join
still grows exponentially. Its remedy offers both a true checkpoint boundary and a stable-base algorithmic rewrite.
When the user runs `structure explain`, risky paths show:

- the optimization that was applied, if any;
- the residual repeated-lineage risk; and
- the nearest actual `checkpoint()` or `local_checkpoint()` operation, or an explicit “none” recommendation.

The warning and explain output must never call cache, persist, alias, Python assignment, or temporary-view reuse a
lineage boundary. `PYSPARK-W2702`, cache-before-checkpoint tracking, and RDD-specific hygiene are out of scope.

## Acceptance Evidence

The focused suite is `tests/specifications/pyspark-lineage-materialization/test_materialization.py`. Its 13 tests cover
operation order and arguments, generated rendering, warning deduplication and reset, project/transform configuration,
fusion-aware warning language, ordinary-versus-Connect capabilities, optimizer lowering, explain lineage state, and
negative non-boundary cases for cache, persist, aliases, Python assignment, and temporary views.

The full local validation completed with 1,674 passed and 66 skipped. The final deterministic verification subset passed
73 with 6 skipped. Flake8, mypy across 1,239 files, package source distribution, and wheel creation also succeeded.

The bounded Docker evidence is:

- PySpark 3.5 checkpoint-every-round: eight rounds, 46–48 characters, `rows=0`.
- PySpark 3.5 fused: seven rounds, plan sizes 760 through 225,286, `rows=0`.
- PySpark 4.0 fused from `/tmp`: four rounds, plan sizes 754 through 17,992, `rows=0`.
- Spark Connect 3.5 smoke: five passed and three skipped; no materialization capability is claimed.

## Related Documents

- End-user troubleshooting: [Spark driver heap exhaustion](../../troubleshooting/memory/spark_driver_heap_oom.gotcha.md)
- Reproducer: [spark_driver_heap_oom.py](../../troubleshooting/memory/spark_driver_heap_oom.py)
- Implementation plan: [P08232601](../planning/P08232601.PySpark-lineage-materialization-and-diagnostics.plan.md)
- Diagnostics catalog: [Diagnostics.md](../../Diagnostics.md)
