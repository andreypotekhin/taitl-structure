# Sprint 00: Groundwork

## Sprint Goal

Create the repository, package layout, configuration model, CLI skeleton, testing infrastructure, and documentation scaffolding needed to implement Structure as a compiler-driven open-source library.

## Product Outcome

A contributor can clone the project, install it in editable mode, run tests, inspect seed configuration defaults, and execute placeholder CLI commands.

## Scope

### In Scope

- Python package skeleton.
- Default project layout: `structure/src` and `structure/generated`.
- Config resolution from defaults, `pyproject.toml`, `structure.toml`, and CLI overrides.
- Seed config file generation.
- CLI skeleton: `check`, `compile`, `explain`.
- Empty generated directory conventions.
- Initial test harness.
- Initial CI script.
- Architecture and implementation docs linked from README.

### Out of Scope

- Actual transform compilation.
- PySpark execution.
- Schema validation runtime.
- Joins, hooks, lineage.

## Relevant Specification Items

- As a developer, I can install Structure as a Python package.
- As a developer, I can use `structure/src` and `structure/generated` by default.
- As a developer, I can override defaults with a small TOML configuration.
- As a developer, I can generate or inspect seed configuration defaults.
- As a developer, I can run `structure check`.
- As a developer, I can run `structure compile` without crashing even before transforms exist.
- As a developer, I can run tests in CI.

## Deliverables

- `structure/` Python package.
- CLI entrypoint.
- `StructureConfig` dataclass or equivalent.
- Config loader.
- `pyproject.seed.toml`.
- Basic logging and diagnostics framework.
- Test directory and first tests.
- CI workflow or documented CI commands.

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

## Acceptance Criteria

- `structure --help` works.
- `structure check` works on an empty project.
- `structure compile` creates or verifies the generated directory.
- Config defaults can be printed or generated.
- Tests pass locally.
- CI can run lint/test commands.

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

## Notes

Keep commands honest. If compilation is not implemented yet, return a clear “no transforms found” result, not a fake success with invented generated code.
