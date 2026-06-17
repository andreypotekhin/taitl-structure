# Milestones

## M0: Groundwork Ready

### Exit Criteria

- Repository layout exists.
- Python package imports successfully.
- CLI skeleton runs.
- Seed TOML config can be loaded.
- Test harness runs in CI.
- Generated output directory conventions are settled around `structure_src` and `structure_generated` defaults.
- Sprint 00 spike notes are captured for decorators, expression helpers, source order, import paths, no-Spark compile, and local Spark execution.
- Any spike result that changes v1 scope is reflected in Sprint 01 before coding begins.
- Architecture docs are linked from project README.

### Demonstration

```bash
structure --help
structure check --src structure_src
pytest
```

## M1: Vertical Slice 1

### Exit Criteria

- A simple schema and transform compile to generated PySpark.
- Generated PySpark class imports successfully.
- Generated transform runs in a local Spark test.
- The compiled path uses `select(...)` and `F.col(...)`, not UDFs.
- Generated code is deterministic and formatted.

### Demonstration

```python
NormalizeOrdersGenerated(spark=spark).run(orders=orders_df)
```

## M2: Schema Enforcement

### Exit Criteria

- Spark `StructType` is generated from schemas.
- `assert_schema(...)` validates inputs and outputs.
- Intermediate validation is enabled by default.
- Class-wide and method-level validation overrides work.
- Invalid schema tests fail predictably.

## M3: Expression DSL Usability

### Exit Criteria

- Common expression functions compile.
- `where(...)` filters compile.
- `@expr_fn` helpers compile and inline.
- Unsupported Python operations fail with structured diagnostics.
- Error messages suggest DSL alternatives, `@expr_fn`, hooks, and config workarounds when applicable.

## M4: Hooks and Generated Classes

### Exit Criteria

- Generated classes are the primary artifacts.
- Hook-free transforms do not import source transform classes.
- Hooked transforms direct-import source class and call hooks.
- Hook signature is validated.
- `@after(method)` and `@before(method)` work.

## M5: Joins, Lineage, Build Integration

### Exit Criteria

- `join_one(...)` compiles to PySpark joins.
- N-step serial joins work across arbitrary named inputs.
- Basic LDJSON lineage is emitted.
- `structure compile --fail-on-diff` works.
- `structure explain` summarizes inputs, steps, filters, joins, hooks, and validation.
