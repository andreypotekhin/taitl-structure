# Troubleshooting

For the end-user reproducer and materialization guidance, see the
[PySpark driver-heap memory gotcha](../troubleshooting/memory/spark_driver_heap_oom.gotcha.md). For the engineering
root-cause, measurements, and implementation contract, see the developer [Memory specification](specifications/Memory.spec.md).

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
Fix: Confirm network access for Docker, then rerun `make integration`. If a PySpark patch version is unavailable,
update `infra/compose/.env` and `infra/compose/.env_example` together and record the change in the active ExecPlan.

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
`validate_intermediate = false` and `connect_plan_boundaries = "auto"`. Setting `validate_intermediate = true` is a
diagnostic opt-in and can recreate the expensive remote-analysis behavior.

For the ordinary-PySpark SearchDocuments reproducer and the measured driver-memory experiment, see
[Gotchas](../Gotchas.md#problem-integration-search-proving-plan-exhausts-the-ordinary-pyspark-driver-heap).

For the self-sufficient PySpark reproducer and end-user restructuring guidance, see [the driver-heap memory gotcha](../troubleshooting/memory/spark_driver_heap_oom.gotcha.md). For root-cause analysis, compile-time detection, warning design, measures, and decisions, see the developer [Memory specification](specifications/Memory.spec.md).

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
