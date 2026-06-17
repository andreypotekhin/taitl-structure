# Sprint 00: Groundwork

## Sprint Goal

Create the repository, package layout, configuration model, CLI skeleton, testing infrastructure, documentation scaffolding, and pre-coding proofs needed to implement Structure as a compiler-driven open-source library.

## Product Outcome

A contributor can clone the project, install it in editable mode, run tests, inspect seed configuration defaults, and execute placeholder CLI commands.

The team can also review short spike notes for the high-risk Python mechanics before Sprint 01 commits to compiler design.

## Scope

### In Scope

- Python package skeleton.
- Source-root discovery for `src` and root-package projects.
- Generated output under `generated/structure_generated`.
- Config resolution from defaults, `pyproject.toml`, `structure.toml`, and CLI overrides.
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
- Spike: compiler check path with no PySpark, SparkSession, Java, or Spark startup.
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
- As a developer, I can generate or inspect seed configuration defaults.
- As a developer, I can run `structure check`.
- As a developer, I can run `structure compile` without crashing even before transforms exist.
- As a developer, I can run tests in CI.
- As a maintainer, I can review spike outcomes before vertical slice coding begins.

## Deliverables

- `structure/` Python package.
- CLI entrypoint.
- `StructureConfig` dataclass or equivalent.
- Config loader.
- `pyproject.seed.toml`.
- Basic logging and diagnostics framework.
- Test directory and first tests.
- CI workflow or documented CI commands.
- Spike notes for decorator mechanics, expression helpers, source ordering, import paths, no-Spark compile, and local Spark execution.

## Engineering Tasks

1. Create package skeleton.
2. Add CLI entrypoint.
3. Implement config defaults.
4. Implement config file discovery.
5. Implement CLI overrides for source and generated directories.
6. Add seed config output command or file.
7. Add project layout docs.
8. Add test harness.
9. Add first CI command list.
10. Add compile-time timing utility placeholder.
11. Spike `@after(method)` inside class bodies.
12. Spike class-local `@expr_fn` helper descriptor behavior.
13. Spike source-order discovery with line numbers.
14. Spike source-root discovery and generated `structure_generated.<source package>` import paths.
15. Spike no-Spark compiler checks.
16. Spike minimal local Spark generated-code execution.

## Acceptance Criteria

- `structure --help` works.
- `structure check` works on an empty project.
- `structure compile` creates or verifies the generated directory.
- Config defaults can be printed or generated.
- Default config uses conventional source-root discovery and `generated/structure_generated`.
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
