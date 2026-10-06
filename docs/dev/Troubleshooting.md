# Troubleshooting

### Problem (runtime): An assertion disappears from Spark's optimized plan

Reproduction: declare `require_all(order.amount >= 0)`, pass a negative amount, and collect the result.
The historical implementation cross-joined a scalar `assert_true` projection and immediately dropped its column.
Spark removed the unused expression, so invalid rows succeeded. The lazy repair makes the assertion column a
filter dependency before dropping it. Relation guards use their declaration input; selected-row ties use windows
over winning candidates. Spark 3.5.0/4.0.0 tests cover collect, count, unrelated projection, and writes in both modes.

A constant-false filter or `limit(0)` can still eliminate the guarded work. This follows the author's chosen lazy
contract; do not restore the provisional first/count actions to force validation during construction. Tests under
`tests/integration/pyspark/book_contracts/` distinguish required evaluated failures from permitted pruning.

### Problem (runtime): A leading raw hook produces a later unresolved-column error

Reproduction: return `orders.drop("amount")` from a leading hook bound to a schema requiring `amount`.
The historical runner waited until a later projection to expose the missing field. Hook recipes now carry their
own output schemas, and each runner validates returned metadata immediately after each hook. The error identifies
the hook, relation, expected field, and remedy. Tests cover missing fields, wrong types, non-DataFrame returns,
tuple arity, extra columns, and projection; no metadata check launches a data action.

### Problem (mypy): Package-exported classes resolve as modules after adding an import

When: An expression helper gains a dependency on a module that already depends on expressions.
Error: Mypy reports that a class is a module, is not valid as a type, or has no expected methods.
Cause: The circular dependency can make same-named package re-exports ambiguous during type checking.
Fix: Confirm with a clean mypy run and, when necessary, `--shadow-file` against the previous source. At affected
internal import sites, import the class from its defining module, such as
`from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan`, while retaining public exports.

For the end-user reproducer and materialization guidance, see the
[PySpark driver-heap memory gotcha](../troubleshooting/memory/spark_driver_heap_oom.gotcha.md). For the engineering
root-cause, measurements, and implementation contract, see the
[resolved OOM issue](issues/I09272602.Spark-driver-heap-exhaustion.issue.md) and its
[Memory evidence](issues/I09272602/Memory-evidence.md). The repeatable investigation method is in
[Memory optimization](optimization/Memory.opt.md).

### Problem (pytest): `PermissionError: [WinError 5] Access is denied: 'C:\Temp\pytest-of-Admin'`

When: Running tests that use pytest's `tmp_path` fixture on a Windows checkout.
Error: `PermissionError: [WinError 5] Access is denied: 'C:\Temp\pytest-of-Admin'`.
Cause: The global pytest temp root exists but is not readable by the current process.
Fix: Set `TMP` and `TEMP` to a writable directory, or use a workspace-local temp directory for tests that only need
short-lived generated files.

### Problem (make gold): `ModuleNotFoundError: No module named 'helpers'` on Windows

When: Regenerating example golden files with `make gold` on Windows.
Error: The regeneration script cannot import the repository's `helpers` package.
Cause: The Makefile target uses the POSIX `:` `PYTHONPATH` separator; Windows requires `;`.
Fix: Run the same target with a Windows path separator:
`$env:PYTHONPATH='.;src;tests;examples/plugins/iterable/src'; poetry run python scripts/regenerate_golden.py`.
Then run `poetry run pytest -q tests/golden` and review the generated diff.

### Problem (Black): formatting hangs on Windows

When: Running `black`, `make format`, or the Black step in `make build`.
Error: Black identifies the input files but makes no progress, while idle Black and Python worker processes remain.
Cause: Black's shared cache is locked or stale.
Fix: Stop the stalled formatter processes and rerun with a fresh cache directory:
`$env:BLACK_CACHE_DIR='.black-cache'; poetry run black src tests`.

### Problem (integration): `docker compose` is not found

When: Running `make integration` or `make build INTEGRATION=1`.
Error: `docker` is not recognized, `docker: command not found`, or `docker compose` exits before reading the Compose
file.
Cause: Docker Desktop or Docker Compose v2 is not installed or is not on `PATH`.
Fix: Install Docker Desktop with Compose v2, start Docker, open a new terminal, and run `docker compose version`.

### Problem (integration): PySpark parity tests are skipped

When: Running `poetry run pytest --run-integration tests/integration/pyspark -q`.
Error: Pytest reports `could not import 'pyspark': No module named 'pyspark'`.
Cause: Live execution/generated-code parity tests need the optional PySpark runtime, which is intentionally not installed for
the default compiler-only test environment.
Fix: Install the project's integration dependencies or run `make integration` in the supported containerized Spark
environment, then rerun the explicit PySpark command. Do not treat the skip as release verification.

### Problem (integration): Docker is not running

When: Running `make integration`.
Error: Docker reports that it cannot connect to the Docker daemon.
Cause: Docker Desktop is installed but the engine is stopped.
Fix: Start Docker Desktop and rerun `make integration`. The integration runner will recreate the Compose stack.

### Problem (integration): Windows Docker pipe access is denied

When: Running `make integration` on Windows.
Error: Docker reports `open //./pipe/docker_engine: Access is denied`.
Cause: The current terminal cannot access the Docker engine pipe.
Fix: Rerun the command from an elevated terminal, or add the user to the local Docker users group and start a new
terminal session.

### Problem (integration): Spark UI or master port is already allocated

When: Starting the local all-version integration stack.
Error: Docker reports that a configured port is already allocated.
Cause: Another process or an older Compose stack is using one of the ports from `infra/compose/.env`.
Fix: Run `docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml down --remove-orphans`.
If the port is still occupied, edit the corresponding port in `infra/compose/.env` and rerun `make integration`.

### Problem (integration): backend container cannot pull or build dependencies

When: Running `make integration` for the first time or after changing PySpark versions.
Error: Docker build fails while installing Java, pytest, or PySpark.
Cause: The Docker build needs network access to operating-system and Python package repositories.
Fix: Confirm network access for Docker, then rerun `make integration`; the image build retries package downloads with a
two-minute timeout. If a PySpark patch version is unavailable, update `infra/compose/.env` and
`infra/compose/.env_example` together and record the change in the active ExecPlan.

### Problem (integration): Docker VM runs out of space while building a backend

When: Running `make integration-rebuild BACKEND=pyspark41` or another image build.
Error: Docker reports `no space left on device`; a later `apt-get update` can also report invalid repository signatures
when it cannot write downloaded metadata.
Cause: Docker's VM filesystem is full, even if the host filesystem has free space. Long-running Spark worker writable
layers can occupy most of it. Spark retains stopped application work directories for seven days by default, so repeated
test runs can accumulate several gigabytes before those directories expire.
Fix: Inspect `docker system df -v` and check free space inside a running container with `docker exec <container> df -h /`.
Finish any active integration jobs before stopping their workers. Remove only unused images or stale containers whose
owners are known, or increase Docker Desktop's disk allocation, then rerun the build. Do not interpret the APT error as a
package signing problem until the VM has free space. Compose workers now retain stopped application data for one hour;
the setting takes effect when each worker is next created. When no integration lane is running, `make integration-down`
is the simplest cleanup: it removes worker containers and their writable layers while preserving the named Spark
Connect dependency caches. If the worker must stay up, inspect the Spark master UI/API first and remove old
`$SPARK_HOME/work/app-*` directories only when the master reports no active applications and the worker has zero cores
in use. Those directories contain executor-local files for completed applications. Do not remove work directories
from an active application. `docker system prune` can reclaim unused build cache and images, but it does not reclaim a
running worker's writable layer; increasing Docker Desktop's disk allocation is the durable option when concurrent
active workloads themselves exceed the VM limit.

### Problem (integration): PySpark 4.0/4.1 state processor rejects the protocol runtime or state store

When: Running the V11 state processor tests on ordinary PySpark 4.0 or 4.1.
Error: Protobuf reports a generated/runtime major-version mismatch, or Spark reports
`STATE_STORE_MULTIPLE_COLUMN_FAMILIES` from the HDFS-backed state store.
Cause: PySpark 4.1's generated state protocol uses Protobuf 6.33.0. Both profiles' TransformWithState state layouts
use multiple column families, which the default HDFS-backed provider does not support.
Fix: Rebuild the affected image with `make integration-rebuild BACKEND=pyspark40` or `BACKEND=pyspark41`. The Compose
build selects Protobuf 6.33.0 for 4.1, leaves the 4.0 Protobuf 5.29.3 pin unchanged, and the V11 fixture selects
`RocksDBStateStoreProvider` for both profiles.

### Problem (integration): Delta Connect 4.1 fails while decoding a server response

When: Starting the `spark-connect41` Delta lane with an older cached server dependency set.
Error: A Delta `detail()` or table operation fails with `MapFieldReflectionAccessor` missing from Protobuf Java.
Cause: The Delta Connect server artifact can resolve Protobuf Java 3.x, which lacks a class used by Spark 4.1's Connect
protocol. The Python client's Protobuf package does not change the server JVM classpath.
Fix: Use the Connect 4.1 launcher in `infra/compose/images/pyspark/run-integration.sh`, which pins
`com.google.protobuf:protobuf-java:4.33.0` beside `delta-connect-server_4.1_2.13:4.1.0`. Rerun
`make integration BACKEND=spark-connect41`; rebuild the image if the launcher itself was changed.

### Problem (integration): PySpark 4.0 cannot import a test processor in the state driver worker

When: Running `transformWithStateInPandas` in the ordinary PySpark 4.0 Compose lane.
Error: The state driver worker fails while unpickling the processor with `ModuleNotFoundError: No module named 'integration'`.
Cause: Spark's state driver worker inherits the integration runner's `PYTHONPATH`; executor `PYTHONPATH` settings and
`SparkContext.addPyFile` do not populate this special worker's import path.
Fix: Use the Compose launcher, which adds `/workspace/tests` to `PYTHONPATH`. Rebuild the runner image after editing
`run-integration.sh` because the launcher script is copied into the image.

### Problem (integration): Spark did not become ready

When: Integration pytest starts but fails before executing the generated transform test.
Error: `Spark did not become ready at spark://...`.
Cause: The Spark master or worker did not start in time, or the runner selected the wrong backend service.
Fix: Rerun `make integration`. For repeated failures, inspect the matching service logs with
`docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml logs spark35-master spark35-worker`
or the `spark40-*` services for the PySpark 4.0 lane.

### Problem (integration): PySpark 4.0 tries to write `/workspace/artifacts`

When: Running a targeted PySpark 4.0 integration command inside the Compose container from the default `/workspace`
directory.
Error: Spark logs `Failed to create directory artifacts/...` and `FileSystemException: /workspace/artifacts:
Read-only file system`; a later setup may report `Only one SparkContext should be running in this JVM`.
Cause: Spark 4.0's artifact manager resolves a relative artifact root under the read-only mounted workspace before the
test can proceed. The failed context startup can leave the JVM in a partially initialized state.
Fix: Run the targeted pytest command from writable `/tmp` and pass the repository pytest config explicitly:
`docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm --workdir /tmp structure-integration-pyspark40 pytest -q /workspace/tests/integration/pyspark/v8/test_stateless_streaming_gaps.py --run-integration -c /workspace/pyproject.toml -rs`.

### Problem (integration): Spark Connect reports `Java heap space`

When: A Spark Connect integration test fails while collecting a generated DataFrame, often after a large search or
similarity query.
Error: The client raises a Spark Connect gRPC exception whose server detail is `Java heap space`; the server trace can
include `TextFormat$TextGenerator`.
Cause: Spark Connect serializes a large logical plan while handling the request. The default Spark driver heap is too
small for some bundled generated-query integration cases.
Fix: The supported runner starts Connect with a 3 GiB driver heap. Rebuild once after updating the runner:
`make integration-rebuild BACKEND=spark-connect35`. If the host has capacity and a larger plan still fails, override
it for that run, for example:
`STRUCTURE_SPARK_CONNECT_DRIVER_MEMORY=3g make integration BACKEND=spark-connect35`.

If the failure occurs after a long chain of intermediate schema checks, verify that Connect is using the default
`validate_intermediate = false` and `plan_boundaries = "auto"`. Setting `validate_intermediate = true` is a
diagnostic opt-in and can recreate the expensive remote-analysis behavior.

For the ordinary-PySpark SearchDocuments reproducer and the measured driver-memory experiment, see the
[Search proving heap evidence](issues/I09272601/Search-proving-heap.evidence.md).

For the self-sufficient PySpark reproducer and end-user restructuring guidance, see [the driver-heap memory gotcha](../troubleshooting/memory/spark_driver_heap_oom.gotcha.md). For root-cause analysis, compile-time detection, warning design, measures, and decisions, see the [resolved OOM issue](issues/I09272602.Spark-driver-heap-exhaustion.issue.md).

### Problem (integration): tiny Search fixtures still exhaust the driver

When: `test_search.py` takes minutes constructing a plan or fails before collecting a handful of rows.
Cause: repeated joins and feedback branches copy upstream logical plans; the fixture's row count is not the relevant
size. Exposing every intermediate stage also analyzes schemas the test does not use. Spark diagnostic plan strings
can amplify memory pressure further.
Fix: shared `snapshots` truncate offline preparation lineage through Parquet, while Search's explicit
`FuseDocuments(materialize=True)` checkpoints selected candidates before feedback branching. Search parity tests
disable stage-output exposure consistently in both modes and reuse generated sources. Keep the sentence UDFs and
expected-value assertions. See [shared integration helpers](Testing.md#reusing-preparation-without-retaining-growing-plans).
For phase timing, controlled repetitions, and benchmark interpretation, see
[Performance troubleshooting](../troubleshooting/performance/Performance.trbl.md).

The runner caps `spark.sql.maxPlanStringLength` at 8192 and uses simple UI explain output. This prevents oversized
diagnostic strings; it does not fix an oversized execution plan. A capped 1 GiB run still exhausted the driver in
closure serialization before the fusion checkpoint was added. Cache, persist, temporary views, and increasing heap
alone are not reliable lineage remedies. Compare individually bounded runs, not concurrent drivers competing within
a small Docker VM.

Spark Connect 4.0 needs `spark.checkpoint.dir` at server startup; setting it through `session.conf.set` fails with
`CANNOT_MODIFY_CONFIG`. The checked-in runner configures and cleans up its own shared-volume directory. Connect 3.5
does not support this boundary; only the full document-search case is excluded there, not the rest of Search.
If an older cached image reports `ModuleNotFoundError: examples` inside a UDF, rebuild it or invoke the checked-in
runner, which explicitly includes `/workspace` in `PYTHONPATH`.
For ordinary Spark, the executor worker needs the same path. Compose sets it explicitly on both worker services;
after updating the Compose file, recreate the affected idle worker with `docker compose ... up -d spark40-worker`
(or `spark35-worker`). Restarting only the test runner does not update an existing worker's environment.

If reranking returns fewer documents after an upgrade, check cached score `scope_id` values against the supplied
document targets. Scores from a different target universe must not be reused. The reranking fixture supplies its
named targets explicitly; this keeps the original three-document and top-result expectations intact.

### Problem (integration): Spark Connect logs `INVALID_HANDLE.SESSION_CLOSED` during `releaseExecute`

When: A Spark Connect 3.5 integration lane finishes a test or the full pytest run.
Error: The Connect server logs `Spark Connect RPC error during: releaseExecute` followed by
`[INVALID_HANDLE.SESSION_CLOSED]`. The pytest progress line may still end in dots and `[100%]`.
Cause: PySpark 3.5 can send a best-effort execution-release request after the corresponding Spark Connect session has
already closed. This is a server-side cleanup race, not a failed Structure transform. A stale integration image can
also expose the server's cleanup output directly.
Fix: If pytest has no `F`, `FAILED`, or nonzero exit status, treat the message as non-fatal. Rebuild the integration
image once to use the quiet Connect runner:
`make integration-rebuild BACKEND=spark-connect35`. Subsequent `make integration BACKEND=spark-connect35` runs reuse
the image and cache. If pytest actually fails, retain the reported traceback; the runner prints the last 200 Connect
server log lines only for a failing test run.

### Problem (integration): Spark Connect hangs entering `test_file_streams.py`

When: Running the Spark Connect 4.0 integration lane; pytest stops after the preceding test and the Connect container
remains alive without progress.
Cause: The file-stream module contains only classic-PySpark tests, but per-test `spark` fixtures were created before
the tests skipped. Spark Connect could then block while tearing down a session after the skip.
Fix: The classic-only streaming modules (v3 file streams, v7/v8 restart coverage, and v10 foreach-batch coverage)
skip at collection time on Spark Connect, so no Connect session is created for those tests. Verify
with `docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm -e
INTEGRATION_PYTEST_ARGS='/workspace/tests/integration/pyspark/v3/streams/test_file_streams.py -q'
structure-integration-spark-connect40`; an expected result is eight skipped stream tests rather than a stalled run.

### Problem (context): `message` during [when]

When: [describe when problem manifests]
Error: [error message]
Cause: [root cause]
Fix: [steps to fix]

### Problem (IDE): `Unexpected type: Expression` on a boolean join predicate

When: A compiled step combines a field comparison with `event_time_between(...)`, for example
`(click.impression_id == impression.impression_id) & event_time_between(...)`.

Error: The IDE reports `Unexpected type: Expression` or flags the `&`/`|` operand even though the expression compiles.

Cause: `event_time_between(...)` intentionally returns Structure's symbolic `Expression`, not Python `bool`. During
authoring, an IDE can infer the field comparison on the left as a Python boolean because schema field declarations are
also used as the compiler's input metadata. The symbolic expression supports reflected `&` and `|` so this mixed
static view remains valid without changing the runtime expression contract.

Fix: Use `&`, `|`, and `~` for symbolic boolean logic. Do not change `event_time_between(...)` to return `bool`, use
Python `and`/`or`/`not`, or add a cast solely to hide this warning. Update Structure if the reflected operators are not
available in the installed version.

### Problem (PMD): 'Double-brace initialization should be avoided' error
When: Running PMD checks as part of the build process.
Error: "[INFO] PMD Failure: [class] :22 Rule:DoubleBraceInitialization Priority:3
Double-brace initialization should be avoided."
Cause: Default PMD rules flag double-brace initialization.
Reference: https://pmd.github.io/pmd/pmd_rules_java_bestpractices.html#doublebraceinitialization
Causing code:

```
public void configure()
{
  Ex.configure()
      .context(new Context("/api/cats") {{
          invariant(new Invariant<Cat>() {{
              create(c -> "Black".equals(c.color), "Cats are born black");
          }});
          ...
```

Workaround 1: Adjust PMD rules.
```
  pmd-ruleset.xml:
    <rule ref="category/java/bestpractices.xml">
        <exclude name="DoubleBraceInitialization" />
```

Workaround 2: Use configure-with-builders style.
```
  Ex.configure()
    .context("/api/cats")
       .invariant(Cat.class)
         .create(c -> "Black".equals(c.color), "Cats are born black")
```
Details: Double-brace initialization creates an anonymous subclass, which is in
line with the code above. It is often overkill for collections, so PMD flags it
by default.
### Problem (build): Black stalls when source and test roots are checked together

When: Running the formatter on Windows with `black src tests`.
Error: Black produces no result and may remain running indefinitely.
Cause: Black's multi-root discovery can stall on this workspace under Windows.
Fix: Run the roots separately: `poetry run black --check src` and `poetry run black --check tests`. The project
`Makefile` uses separate invocations for both formatting and lint checks. If a previously timed-out Black process left
the cache unusable, retry with a fresh temporary cache:
`$env:BLACK_CACHE_DIR=Join-Path $env:TEMP 'structure-black-cache'; make build`.

### Problem (mypy): `import-untyped` from a local package on macOS or Linux

When: Running `poetry run mypy src tests` or `make build` on a case-sensitive filesystem.
Error: Mypy reports `Skipping analyzing "...Capabilities": module is installed, but missing library stubs or py.typed marker`.
Cause: A package `__init__.py` imports a local module with filename casing that does not match the real file on disk.
Windows can hide this because its default filesystem is case-insensitive.
Fix: Make the import path match the actual filename exactly. For example, import
`structure.app.target.capabilities.api.capabilities` instead of
`structure.app.target.capabilities.api.Capabilities`.
