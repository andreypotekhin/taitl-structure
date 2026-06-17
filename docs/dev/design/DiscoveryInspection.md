# Design: Discovery and Inspection

## Purpose

Discovery locates transform classes and schema definitions, preserving enough source information to compile and report useful errors.

## Inputs

- `source_dir`
- `source_package`
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
2. Find Python files under source_dir/source_package.
3. Import modules or inspect source safely.
4. Find classes marked with @transform.
5. Read class __dict__ order.
6. Identify input declarations.
7. Identify @expr_fn helpers.
8. Identify public schema-returning subtransform methods.
9. Identify @before(method) and @after(method) hooks.
10. Attach line numbers and source snippets when available.
```

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
