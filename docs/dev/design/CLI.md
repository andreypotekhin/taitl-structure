# Design: CLI

## Purpose

The CLI provides local development, CI, and build integration.

## Commands

```bash
structure init
structure check
structure compile
structure compile --fail-on-diff
structure explain orders.transforms.order.EnrichOrders
structure clean
```

## `check`

Runs discovery, symbolic execution, and compileability checks without writing generated files.

## `compile`

Writes generated schemas, transforms, runtime support, compiler provenance, and static dataflow lineage.

## `--fail-on-diff`

Generates into a temporary directory, compares with checked-in generated output, and fails if files differ.

## `explain`

Shows what the compiler sees:

```text
EnrichOrders
  inputs:
    orders: OrderRaw
    customers: Customer

  steps:
    normalize: OrderRaw -> OrderNormalized
      filters: 3
      hooks: after remove_negative_totals
      validates output: yes
```

## `--profile`

Reports compile-time performance metrics:

- discovery time
- symbolic execution time
- checking time
- codegen time
- formatting time
- compiler provenance time
- static dataflow lineage time
- total time
- files written
- cache hits

## Config Resolution

1. CLI flags
2. `pyproject.toml` `[tool.structure]`
3. `structure.toml`
4. defaults

## Compile-Time Performance

CLI should expose profiling and support incremental compile by default.
