# Sprint 20: V5 PySpark Plugin Extraction

## Sprint Goal

Move PySpark target ownership behind the public Platform API while preserving the complete released PySpark contract.

## Product Outcome

PySpark users retain online and generated behavior, but Core no longer imports PySpark plans, runners, renderers,
capability rules, or PySpark platform DSL directly.

## Scope

### In Scope

- `structure.platform.pyspark` platform DSL and field/type definitions.
- PySpark `PlatformAPI` façade with schema, compiler, capability, execution, and generation service facets.
- Generic `StructureSession(runtime=..., context=...)` execution.
- Callback-backed CLI check, compile, explain, schema tooling, traceability, and generated-file workflows.
- PySpark classic and Connect regression, parity, and Spark-free compilation evidence.

### Out of Scope

- Root-export removal before replacement imports and fixtures are ready.
- New PySpark transformation families unrelated to extraction.
- External plugin conformance documentation.

## ExecPlan

`docs/dev/planning/P07162601.V5-platform-callback-architecture.plan.md`

## Acceptance Criteria

- PySpark uses the same public Platform API exposed to external distributions.
- Core artifact and runtime modules contain no concrete PySpark plan or runtime types.
- Existing supported PySpark semantics and generated output remain equivalent.
- Compiler-only commands remain free of PySpark, Java, and Spark startup.
- `make build` and supported PySpark integration lanes pass.

## Progress

- [ ] Start after Sprint 19 closes.
