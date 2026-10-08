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
concept suites after separate Iceberg and Delta provider passes. Those providers are excluded from the general pass.
The optional Sedona geometry test remains scoped to the configured 3.5/4.0 provider lanes; 4.1 does not configure its
SQL extension. Ordinary-only streaming, state processor, and sink tests keep their existing exclusions.
The tracked
environment template pins PySpark 4.1.0 and separate 4.1 ports; Compose uses those values as defaults if an existing
untracked `.env` predates this lane. The 4.1 image also uses Protobuf 6.33.0 to match the generated state protocol
bundled with PySpark 4.1; 3.5 and 4.0 keep the shared Protobuf 5.29.3 pin. The PySpark 4.0 Pandas state API and both
4.1 state processor APIs require pandas, PyArrow, and Protobuf in the driver and worker environments. The 4.0/4.1
state test sessions use Spark's RocksDB state store because these APIs create multiple state column families.
The Delta live module runs first in its own Python process on each classic lane so Delta's Ivy-resolved jars are on the
driver classpath before another PySpark test initializes the JVM. The image pins PySpark 3.5.3 with Delta 3.3.3,
PySpark 4.0.0 with Delta 4.0.1, and PySpark 4.1.0 with Delta 4.1.0. The remaining V11 tests run in a fresh process.

The Spark Connect lanes are experimental. They start the Spark Connect gateway inside the test runner container and do
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
