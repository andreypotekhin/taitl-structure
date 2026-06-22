# Local Integration Infrastructure

This directory contains the Docker Compose stack for Structure integration tests. The default stack starts every
currently claimed PySpark backend version at the same time, with distinct services and host ports.

## Environment

Local settings live in `infra/compose/.env`. The tracked template is `infra/compose/.env_example`.

Create `.env` automatically:

    poetry run python scripts/ensure_compose_env.py

The command is safe to rerun. It never overwrites an existing `.env`.

## Run Tests

Run the full matrix:

    make integration

Run one backend's test selection against the all-version stack:

    make integration BACKEND=pyspark35
    make integration BACKEND=pyspark40

Include integration tests after the ordinary build:

    make build INTEGRATION=1

Plain `make build` and `poetry run pytest` stay Spark-free and do not start Docker.
