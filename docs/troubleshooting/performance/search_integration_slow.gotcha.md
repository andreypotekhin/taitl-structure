# Search integration is slow with a tiny fixture

## Symptoms

**When:** A Search integration test runs both online and generated execution against a small document/query fixture.

**What you see:** The test appears hung before it returns a few rows. Once the query is ready, collecting the rows is
often fast. In severe cases the driver reports `java.lang.OutOfMemoryError: Java heap space`, or Spark Connect fails
while sending a large plan.

**Why it happens:** Search performs several steps over lazy DataFrames. The amount of input data can be small even when
the work needed to build the query is large. Asking for intermediate results can add more work.

The detailed troubleshooting method is in [Performance.trbl.md](Performance.trbl.md), and the reproducible performance
sequence is in the [Performance runbook](../../runbooks/Performance.runbook.md).

## Compile setup failure

If the test stops during fixture setup with an error such as
`union_all(relation) requires a Structure relation parameter or transform input`, this is a compiler/DSL binding
failure, not a slow Spark action or a cache miss. The usual cause is a helper passing a row-scoped value to a relation
set operation. `union_all`, `union_by_name`, and similar operations require a declared Structure relation input.

Fix the transform signature and call site so the relation is declared as a transform input (or pass the declared
relation directly). When combining internal `lane(...)` outputs, use lane-compatible joins/filters or promote the
relations to real transform inputs; do not pass a lane-scoped `RowScope` to `union_all`. Then rerun the focused test
with compiler profiling enabled. Do not hide the failure by switching artifact-reuse policy, adding Spark actions, or
treating the failed setup timer as a performance sample. Once the test completes, compare `off` and `module` in fresh
runner processes as described below.

## Remedies

For a focused test that only needs final Search results:

- Turn off unused intermediate stage outputs with `STRUCTURE_SEARCH_STAGE_OUTPUTS=0`.
- For batch execution, use the normal automatic shared-plan boundaries with `plan_boundaries="auto"`.
- Keep the standard Search pipeline and its conditional `FuseDocuments` materialization policy; do not add caches or
  checkpoints everywhere as a first response.
- Keep the default module-owned compiled-artifact reuse for normal integration runs. To test whether repeated Structure
  compilation contributes to the delay, compare `STRUCTURE_COMPILED_ARTIFACT_REUSE=module` with `off` and enable
  `STRUCTURE_PROFILE_COMPILATION=1`. Keep the generated package and compiler settings identical; this control does not
  remove Spark plan construction, checkpoint work, or collection time.
- If the test is still slow, use [Performance troubleshooting](Performance.trbl.md) to identify which phase is taking
  the time. If the driver reports a heap error, also see the
  [driver-heap gotcha](../memory/spark_driver_heap_oom.gotcha.md).

Keep final input/output validation enabled. Changes that make a test faster still need to preserve ranking, schemas, and
online/generated results; the [Performance runbook](../../runbooks/Performance.runbook.md) explains how to check that.

## References

- [Performance troubleshooting](Performance.trbl.md)
- [Performance runbook](../../runbooks/Performance.runbook.md)
- [Compiled-artifact profiling](../../runbooks/Profiling.runbook.md#compiled-artifact-reuse)
- [Driver-heap memory gotcha](../memory/spark_driver_heap_oom.gotcha.md)
