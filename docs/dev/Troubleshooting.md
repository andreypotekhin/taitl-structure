# Troubleshooting

### Problem (pytest): `PermissionError: [WinError 5] Access is denied: 'C:\Temp\pytest-of-Admin'`

When: Running tests that use pytest's `tmp_path` fixture on a Windows checkout.
Error: `PermissionError: [WinError 5] Access is denied: 'C:\Temp\pytest-of-Admin'`.
Cause: The global pytest temp root exists but is not readable by the current process.
Fix: Set `TMP` and `TEMP` to a writable directory, or use a workspace-local temp directory for tests that only need
short-lived generated files.

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

### Problem (context): `message` during [when]

When: [describe when problem manifests]
Error: [error message]
Cause: [root cause]
Fix: [steps to fix]

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
