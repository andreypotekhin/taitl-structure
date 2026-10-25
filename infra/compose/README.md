# Local Integration Infrastructure

This directory contains the Docker Compose stack for Structure integration tests. A run starts only the Spark version
needed by its selected backend and leaves that local service available for the next run.

## Environment

Local settings live in `infra/compose/.env`. The tracked template is `infra/compose/.env_example`.

Create `.env` automatically:

    poetry run python scripts/ensure_compose_env.py

The command is safe to rerun. It never overwrites an existing `.env`.

## Run Tests

Run the full matrix:

    make integration

Run one backend's test selection:

    make integration BACKEND=pyspark35
    make integration BACKEND=pyspark40
    make integration BACKEND=pyspark41
    make integration BACKEND=spark-connect35
    make integration BACKEND=spark-connect40
    make integration BACKEND=spark-connect41

The ordinary PySpark 4.1 runner executes only the backend version check and `tests/integration/pyspark/v11`. It does
not run the 3.5/4.0 regression tree or the concept tests. The Connect 4.1 runner runs the full integration and live
concept suites after separate Iceberg and Delta provider passes. Those providers are excluded from the general passes.
The legacy Sedona geometry test uses the configured 3.5/4.0 provider lanes. The V11 geospatial tests separately create
isolated native Apache and Sedona sessions, covering ordinary/Connect 4.1 native helpers and Sedona 1.9.0 on ordinary/
Connect 3.5, 4.0, and 4.1. The ordinary 4.1 service does not install Sedona's SQL extension globally because it replaces
native `st_*` registrations. Managed Databricks evidence requires caller-supplied sessions. Ordinary-only streaming,
state processor, and sink tests retain their documented exclusions.
The tracked environment template pins PySpark 4.1.0 and separate 4.1 ports; Compose uses those values as defaults if
an existing
untracked `.env` predates this lane. The 4.1 image also uses Protobuf 6.33.0 to match the generated state protocol
bundled with PySpark 4.1; 3.5 and 4.0 keep the shared Protobuf 5.29.3 pin. The PySpark 4.0 Pandas state API and both
4.1 state processor APIs require pandas, PyArrow, and Protobuf in the driver and worker environments. The 4.0/4.1
state test sessions use Spark's RocksDB state store because these APIs create multiple state column families.
After Iceberg's isolated pass, the Delta live module runs in its own Python process on each ordinary lane so its jars
are on the
driver classpath before another PySpark test initializes the JVM. The image pins PySpark 3.5.3 with Delta 3.3.3,
PySpark 4.0.0 with Delta 4.0.1, and PySpark 4.1.0 with Delta 4.1.0. The remaining V11 tests run in a fresh process.

The Spark Connect lanes verify the admitted batch feature set. They start the gateway inside the test runner container
and do
not add separate Connect services to the Compose stack. The gateway defaults to a 3 GiB driver heap, which can be
overridden with `STRUCTURE_SPARK_CONNECT_DRIVER_MEMORY` for constrained or larger local environments.
The Connect 4.1 lane runs that gateway with `local[2]`; selecting it alone does not start Spark master and worker
containers.
The Connect 4.1 gateway loads `delta-connect-server_4.1_2.13:4.1.0` with Protobuf Java 4.33.0, Delta's SQL extension
and catalog, and the Delta relation and command plugins. The explicit Protobuf pin keeps the server compatible with
Spark 4.1's Connect protocol. Its Python client uses the matching `delta-spark` package. The server settings
are required for external Connect deployments using Structure's Delta helpers.
See [Delta Connect server troubleshooting](../../docs/dev/Troubleshooting.md#problem-integration-delta-connect-41-fails-while-decoding-a-server-response)
if a cached server reports a Protobuf class error.

Connect 3.5 and 4.0 do not run the Iceberg or Delta integration modules: those profiles have no isolated provider
server setup or admitted provider contract. The launcher excludes those modules from their general test selection.
Connect 4.1 retains the isolated provider passes and excludes them from its general passes.

Ordinary PySpark runs use the JVM default driver heap unless `STRUCTURE_SPARK_DRIVER_MEMORY` is set. For a bounded
diagnostic run, set it in `infra/compose/.env` or pass it to the runner, for example
`docker compose ... run --rm -e STRUCTURE_SPARK_DRIVER_MEMORY=3g structure-integration-pyspark35`. The runner applies
this before launching PySpark through `PYSPARK_SUBMIT_ARGS`; changing `spark.driver.memory` after the session starts is
too late to enlarge the driver JVM.

All six Compose runners invoke the repository-mounted Bash launcher. Launcher changes take effect without rebuilding
the cached dependency image. See [cached runner troubleshooting](../../docs/dev/Troubleshooting.md#problem-integration-a-cached-runner-selects-outdated-tests)
if a run selects fewer modules than expected. Each run records the launcher checksum and runtime package versions; each test phase
records its selection, exit status, and pytest skip reasons. Iceberg and Delta run in separate processes before the
general suite. Connect 4.0 and 4.1 each receive a unique temporary checkpoint directory, removed together with their
gateway when the runner exits, including after test failure or timeout.

Each pytest phase has a 3,600-second default deadline. A timeout is an incomplete run, not a skip or passing evidence.
Full general selections run as three separate pytest processes: `general-contracts` covers `book_contracts`;
`general-examples` covers `search`, `security`, `stocks`, and `store`; `general-runtime` covers the remaining integration
tree and live concepts, excluding those directories and the isolated providers. The partitions retain the full
selection without duplicate execution. Ordinary 4.1 retains its single V11-only `general` phase. Each phase prints
its start time, elapsed seconds, individual test names, twenty slowest durations, and skip reasons, so a timeout can
be traced to the test in progress rather than inferred from the percentage of collected tests completed.

To retry one independent pass without repeating providers, select its phase:

```sh
STRUCTURE_INTEGRATION_PHASE=general-contracts make integration BACKEND=pyspark35
```

`all` is the default; `iceberg`, `delta`, `general`, and the three `general-*` names select individual phases or,
for `general`, all general passes. Unsupported phase/backend combinations fail before starting a test session or
Connect gateway. A selected phase is partial evidence; only all configured phases complete a lane. The orchestrator forwards
`STRUCTURE_INTEGRATION_PHASE`, `INTEGRATION_PYTEST_ARGS`, and `STRUCTURE_INTEGRATION_TIMEOUT` from the host when set.
For a focused diagnostic run, pass the filter and optional deadline through the orchestrator or Compose `run -e`:

```sh
docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm \
  -e 'INTEGRATION_PYTEST_ARGS=-k test_runtime_versions' \
  -e STRUCTURE_INTEGRATION_TIMEOUT=600 structure-integration-spark-connect41
```

Provider phases still collect; when an explicit filter selects no provider tests, that phase reports deselection and
continues. Include provider test names in the filter when their execution is required.

The integration orchestrator starts only services required by the selected backends and uses Compose `--no-recreate`
so already-running shared Spark services keep their containers and uptime during another lane's run.
Phases and lanes execute sequentially. Ordinary test applications can claim every core on their shared standalone
worker; starting another pass there can leave it waiting for executor resources. Parallel execution requires separate
workers or explicitly bounded application resources, enough combined driver/executor memory, and isolated provider
catalogs and checkpoint storage. Container separation alone does not provide that isolation.
For unattended runs on macOS, `caffeinate -i make integration BACKEND=pyspark35` prevents idle sleep for the command's
lifetime without changing persistent power settings. A sleeping host can pause Docker and make launcher wall time
substantially longer than pytest's measured execution time; retain both when comparing runs.

The test runner is removed after every run, while the Spark master/worker services and the versioned Spark Connect Ivy
caches are retained locally. This avoids repeat image builds, Spark startup, and Spark Connect dependency downloads.
Use `make integration-rebuild` after changing image dependencies or configuration; it explicitly builds the selected
runner images before starting their required services and running tests. Use `make integration-down` to stop retained
services without deleting the dependency caches. Docker's normal `docker compose ... down -v` removes those caches and
forces the Spark Connect dependencies to download again.

Spark standalone workers clean stopped-application directories every 15 minutes and retain them for one hour. Spark's
default retention is seven days, which allowed repeated integration runs to fill the Docker VM before cleanup began.
After changing this worker setting, recreate an idle worker with `docker compose --env-file infra/compose/.env -f
infra/compose/docker-compose.yaml up -d --force-recreate spark40-worker` (substitute the backend worker). Do not
recreate a worker while it has active executors.

Include integration tests after the ordinary build:

    make build INTEGRATION=1

Plain `make build` and `poetry run pytest` stay Spark-free and do not start Docker.
