# Source Module Rules

## Purpose

Structure discovers user schemas and transforms from ordinary Python projects. Discovery must fit common source layouts,
produce deterministic import paths, and stay safe enough for compiler commands that run in local development and CI.

This specification owns source-root resolution, import safety, module discovery, generated import mapping, source-order
metadata, and diagnostics around unsafe or ambiguous source modules.

## Source Roots

Source roots are filesystem directories that contain importable user modules. They are not package names.

Resolution order:

1. CLI flags.
2. `[tool.structure].source_roots` in `pyproject.toml`.
3. `source_roots` in `structure.toml`.
4. `["src"]` when `./src` exists and contains importable packages or modules.
5. `["."]`.

Rules:

- Source roots are interpreted relative to the project root unless absolute paths are explicitly allowed later.
- Resolved source roots must exist.
- A source root may contain several packages or modules.
- A source root must not be inside `generated_dir`.
- Duplicate source roots after path normalization are collapsed.
- Source roots must be ordered deterministically.
- The compiler must report which source roots were selected in debug or explain output.

## Import Paths

For a source file under a source root, Structure derives the module import path from the path below that root.

Examples:

```text
src/orders/transforms/order.py
  -> orders.transforms.order

orders/transforms/order.py
  -> orders.transforms.order
```

Rules:

- `__init__.py` maps to the package module.
- Files and directories that are not valid Python module names are ignored unless explicitly included later.
- If the same import path is found under multiple source roots, discovery fails with an ambiguity diagnostic.
- Generated modules mirror the source import path below the generated package.

Generated path mapping:

```text
src/orders/transforms/order.py
  -> generated/structure_generated/orders/pyspark/transforms/order.py

orders/transforms/order.py
  -> generated/structure_generated/orders/pyspark/transforms/order.py
```

The default generated package is `structure_generated`.

## Import-Safe Modules

Structure v1 may import user source modules to discover `Structure` and `Transform` classes. Therefore user modules
must be import-safe.

Import-safe means importing the module only declares Python objects and does not perform application work.

Allowed at module import:

- imports of standard library and project modules;
- schema class declarations;
- transform class declarations;
- `@expr_fn` declarations;
- constants that are cheap and deterministic;
- type aliases and helper functions.

Disallowed at module import:

- creating Spark sessions;
- reading data files;
- writing files;
- opening database or network connections;
- starting jobs, threads, or services;
- calling external services;
- executing Spark actions;
- parsing large datasets;
- depending on environment-specific secrets for discovery.

The compiler is not required to undo side effects caused by unsafe imports. Diagnostics and documentation must make the
rule explicit.

## Discovery

Discovery finds classes marked by `@transform` and schema classes that are reachable from those transforms or imported
directly from source modules.

Rules:

- Only classes decorated with `@transform` are compiled as transform entrypoints.
- A class inheriting `Transform` without `@transform` is not compiled unless a later explicit registration mode exists.
- Schema classes may be discovered from transform inputs, subtransform annotations, nested schema types, and direct
  source scans when supported.
- Private modules are not automatically excluded. A later config option may add include and exclude patterns.
- Discovery order is deterministic by source root order, path sort order, then module import path.
- Source location metadata should include project-relative path and line number when available.
- Discovery must not import PySpark, start Java, create Spark sessions, or inspect live DataFrames.

## Source Order

Structure uses Python class-body order for:

- schema fields;
- transform inputs;
- compiled subtransforms;
- expression helpers;
- hooks attached to the same subtransform and timing;
- validation decorators.

Rules:

- Source order must be captured during class creation or through inspectable metadata.
- If source order cannot be recovered for a required ordering decision, discovery fails with a diagnostic.
- Generated output and diagnostics must be deterministic for the same source.

## Reloading and Caching

Compiler commands may cache source fingerprints, discovered metadata, and IR. Cache entries must be invalidated when:

- a source file changes;
- configuration changes;
- Structure version changes;
- generated package or backend target changes;
- a dependency that participates in discovered symbols changes, when detectable.

v1 may implement conservative full rediscovery instead of incremental caching. The implementation must not bake in a
design that prevents v2 incremental compilation.

## Diagnostics

Discovery diagnostics must include:

- project root;
- selected source root;
- source path or module path;
- conflicting module path when relevant;
- problem;
- suggested fix;
- documentation link.

Examples:

```text
CompileError DISC-E0201: Source root does not exist

Setting:
  [tool.structure].source_roots = ["pipeline_src"]

Problem:
  The configured source root pipeline_src was not found under the project root.

Use:
  Create the directory, correct the path, or remove the setting to use the default source-root discovery.

See docs/specifications/SourceModuleRules.md
```

```text
CompileError DISC-E0202: Source module is not import-safe

Module:
  orders.transforms.order

Problem:
  Importing this module raised an exception during discovery.

Use:
  Move Spark session creation, data reads, network calls, and other side effects behind runtime functions or hooks.

See docs/specifications/SourceModuleRules.md
```

```text
CompileError DISC-E0203: Ambiguous source module

Module:
  orders.transforms.order

Problem:
  The same import path exists under more than one source root.

Use:
  Remove one source root, rename one package, or configure source_roots so each import path is unique.

See docs/specifications/SourceModuleRules.md
```

## Implementation Checklist

1. Resolve project root and configuration before discovery.
2. Implement source-root resolution in the required order.
3. Normalize and validate source roots.
4. Enumerate candidate Python modules deterministically.
5. Derive import paths from source-root-relative paths.
6. Detect duplicate import paths.
7. Import source modules without importing PySpark through Structure internals.
8. Record discovered schemas, transforms, source paths, and line numbers.
9. Preserve class-body order for fields, inputs, subtransforms, helpers, and hooks.
10. Map source import paths to generated import paths.
11. Add diagnostics with links to this specification.
12. Add tests for `src` layout, root-package layout, duplicate paths, unsafe imports, and generated path mapping.

## Acceptance Criteria

- With no config and an importable `src` directory, `source_roots` resolves to `["src"]`.
- With no config and no importable `src` directory, `source_roots` resolves to `["."]`.
- CLI `source_roots` override project configuration.
- `[tool.structure].source_roots` overrides `structure.toml`.
- Missing source roots fail with an actionable diagnostic.
- A source root inside `generated_dir` fails.
- The same module import path under two roots fails.
- `src/orders/transforms/order.py` maps to `orders.transforms.order`.
- Generated output for that module maps below `generated/structure_generated/orders/pyspark/transforms/order.py`.
- Importing user modules for discovery does not import PySpark through Structure internals.
- Unsafe import failures point users to this specification.
- Class-body source order is preserved for fields, inputs, subtransforms, hooks, and helpers.
