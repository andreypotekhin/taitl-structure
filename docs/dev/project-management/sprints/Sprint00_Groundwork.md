# Sprint 00: Groundwork

## Sprint Goal

Create the repository, package layout, configuration model, CLI skeleton, testing infrastructure, documentation
scaffolding, and pre-coding proofs needed to implement Structure as an IR-first open-source library with online
execution as the v1 default.

## Product Outcome

A contributor can clone the project, install it in editable mode, run tests, inspect seed configuration defaults, and execute placeholder CLI commands.

The team can also review short spike notes for the high-risk Python mechanics before Sprint 01 commits to compiler design.

## Scope

### In Scope

- Python package skeleton.
- Source-root discovery for `src` and root-package projects.
- Generated output under `generated/structure_generated`.
- Online execution mode default.
- Config resolution across CLI overrides, `pyproject.toml`, `structure.toml`, and built-in defaults.
- Explicit config precedence: CLI flags, `[tool.structure]` in `pyproject.toml`, `structure.toml`, built-in defaults.
- Config schema validation for unknown keys and invalid values.
- Structured config diagnostics, including allowed values for enum-like settings such as lineage.
- Compatibility policy for Python, PySpark, generated code, compiler lineage schema, and config schema.
- Seed config file generation.
- CLI skeleton: `check`, `compile`, `explain`.
- Empty generated directory conventions.
- Initial test harness.
- Initial CI script.
- Architecture and implementation docs linked from README.
- Spike: `@after(method)` binding inside class bodies.
- Spike: class-local `@expr_fn` helpers callable through `self` without a `self` parameter.
- Spike: source-order discovery with stable line numbers.
- Spike: source-root discovery and generated `structure_generated.<source package>` import paths.
- Spike: `StructureSession` and deferred transform invocation API.
- Spike: compiler check and compile paths with no PySpark, Java, SparkSession, Spark startup, or Spark cluster.
- Spike: minimal generated PySpark execution test with local Spark.

### Out of Scope

- Production transform compilation beyond the minimal spike fixture.
- Production PySpark execution support beyond the minimal spike fixture.
- Schema validation runtime.
- Joins, hooks, lineage.
- Full implementation of any spike subject beyond the proof needed for Sprint 01 planning.

## Relevant Specification Items

- As a developer, I can install Structure as a Python package.
- As a developer, I can rely on conventional source-root discovery by default.
- As a developer, I can override defaults with a small TOML configuration.
- As a developer, I can rely on explicit configuration precedence.
- As a developer, I can receive structured diagnostics for invalid configuration.
- As a developer, I can rely on documented Python and PySpark support ranges.
- As a developer, I can configure the generated PySpark target range.
- As a developer, I can inspect the default online execution mode.
- As a developer, I can generate or inspect seed configuration defaults.
- As a developer, I can run `structure check`.
- As a developer, I can run `structure compile` without crashing even before transforms exist.
- As a developer, I can run tests in CI.
- As a maintainer, I can review spike outcomes before vertical slice coding begins.

## Deliverables

- `structure/` Python package.
- CLI entrypoint.
- `StructureConfig` dataclass or equivalent.
- `execution_mode = "online"` default.
- Config loader.
- `pyproject.seed.toml`.
- Basic logging and diagnostics framework.
- Public compatibility policy.
- Test directory and first tests.
- CI workflow or documented CI commands.
- Spike notes for decorator mechanics, expression helpers, source ordering, import paths, no-Spark compile, and local
  Spark execution.

## Engineering Tasks

1. Create package skeleton.
2. Add CLI entrypoint.
3. Implement config defaults.
4. Implement config file discovery.
5. Implement explicit config precedence.
6. Implement config schema validation.
7. Add structured config diagnostics for unknown keys and invalid values.
8. Implement compatibility config defaults and validation.
9. Implement CLI overrides for source and generated directories.
10. Add seed config output command or file.
11. Add project layout docs.
12. Add test harness.
13. Add first CI command list.
14. Add compile-time timing utility placeholder.
15. Spike `@after(method)` inside class bodies.
16. Spike class-local `@expr_fn` helper descriptor behavior.
17. Spike source-order discovery with line numbers.
18. Spike source-root discovery and generated `structure_generated.<source package>` import paths.
19. Spike no-Spark compiler checks and compile.
20. Spike `StructureSession` and deferred transform invocation API.
21. Spike minimal local Spark generated-code execution.

## Acceptance Criteria

- `structure --help` works.
- `structure check` works on an empty project.
- `structure compile` creates or verifies the generated directory.
- `structure check` and `structure compile` work without PySpark, Java, SparkSession, Spark startup, or a Spark cluster.
- Config defaults can be printed or generated.
- Default config uses conventional source-root discovery and `generated/structure_generated`.
- Default config uses `execution_mode = "online"`.
- Config precedence is documented and covered by tests.
- Unknown config keys fail with structured diagnostics.
- Invalid config values fail with allowed values when applicable.
- Compatibility policy is documented and reflected in seed config defaults.
- Tests pass locally.
- CI can run lint/test commands.
- Spike notes are committed and linked from Sprint 01 planning.
- Sprint 01 scope reflects any spike-driven design changes.

## Demo Script

```bash
pip install -e .
structure --help
structure check
structure compile
pytest
```

## Compile-Time Performance Metric

Capture baseline CLI startup and empty-project check time.

Target for this sprint:

- Empty `structure check`: under 1 second on a normal development machine.

## Risks

- Config scope may grow too quickly.
- Default paths may confuse IDEs if not documented.
- CLI commands may imply behavior not yet implemented.
- Spikes may reveal syntax or layout changes that should block Sprint 01 until resolved.

## Notes

Keep commands honest. If compilation is not implemented yet, return a clear “no transforms found” result, not a fake success with invented generated code.
