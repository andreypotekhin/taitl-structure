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
    make integration BACKEND=spark-connect35
    make integration BACKEND=spark-connect40

The Spark Connect lanes are experimental. They start the Spark Connect gateway inside the test runner container and do
not add separate Connect services to the Compose stack.

The test runner is removed after every run, while the Spark master/worker services and the versioned Spark Connect Ivy
caches are retained locally. This avoids repeat image builds, Spark startup, and Spark Connect dependency downloads.
Use `make integration-rebuild` after changing a Compose image, and `make integration-down` to stop the retained
services without deleting the dependency caches. Docker's normal `docker compose ... down -v` removes those caches and
forces the Spark Connect dependencies to download again.

Include integration tests after the ordinary build:

    make build INTEGRATION=1

Plain `make build` and `poetry run pytest` stay Spark-free and do not start Docker.
