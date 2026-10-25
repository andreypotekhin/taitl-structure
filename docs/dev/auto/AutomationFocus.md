## Automation Focus

Sprints: docs/dev/project/sprints/

### Current iteration
Sprint: 61 V12 admission and runtime foundation

### Next planned release

[V12](../project/V12.md): twelve two-week sprints, 61–72, for PySpark 4.2 adoption and deeper V11 coverage.
Spark Connect remains batch-only; ordinary streaming is extended. Sprint 61 preparation is complete and runtime
implementation is pending. Calendar dates remain unset. Planning documents do not mark unimplemented capabilities
supported.

Validation cadence: `make build` after each coding task, discretionary focused live checks during
development, and `make build INTEGRATION=1 BACKEND=all` at each sprint close. See
[Testing](../Testing.md#validation-cadence) for acceptance and evidence requirements.
