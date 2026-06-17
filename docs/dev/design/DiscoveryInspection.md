# Design: Discovery and Inspection

## Purpose

Discovery locates transform classes and schema definitions, preserving enough source information to compile and report useful errors.

## Inputs

- `source_roots`
- Python module files
- optional config and CLI overrides

## Outputs

- discovered schema classes
- discovered transform classes
- ordered transform members
- source locations
- hook associations

## Process

```text
1. Load configuration.
2. Resolve source roots from config or convention.
3. Find Python files under each source root.
4. Import modules or inspect source safely.
5. Find classes marked with @transform.
6. Read class __dict__ order.
7. Identify input declarations.
8. Identify @expr_fn helpers.
9. Identify public schema-returning subtransform methods.
10. Identify @before(method) and @after(method) hooks.
11. Attach line numbers and source snippets when available.
```

## Source Root Resolution

Explicit configuration wins. Without config, discovery uses this convention:

1. If `./src` exists and contains importable packages or modules, use `["src"]`.
2. Otherwise, use `["."]`.

Generated artifact paths mirror module paths relative to the selected source root. The physical root name
itself is not part of the module path.

## Source Order

Python class dictionaries preserve definition order. Structure should use this order as the default subtransform execution order.

## Error Cases

- public method without schema return annotation
- hook references method not in same class
- duplicate input name
- duplicate generated method name
- source order does not match schema flow

## Compile-Time Performance

Discovery is often the largest compile-time cost for large projects. Use source fingerprints, avoid repeated imports, cache module inspection results, and support incremental compile.
