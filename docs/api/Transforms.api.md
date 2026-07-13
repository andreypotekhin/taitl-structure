# Transforms API

These declarations and operations define compiler-visible transform methods. Examples assume declared `OrderRaw`,
`OrderClean`, `OrderPublished`, and `order` schemas or row scopes.

## Simple Transform Declarations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `Transform` | DataFrame pipeline class | `class Publish(Transform): pass` |
| `input(...)` | DataFrame input | `orders = input(OrderRaw)` |
| `output(...)` | DataFrame result | `published = output(OrderPublished)` |
| `lane(...)` | Intermediate DataFrame | `clean = lane(OrderClean)` |
| `@transform(...)` | Pipeline declaration | `@transform\nclass Publish(Transform): pass` |
| `@step(...)` | Named pipeline step | `@step(inout=lane(clean) \| output(published))` |

**Details And Differences**

- `input(...)`, `output(...)`, and `lane(...)` declare named transform boundaries.
- Undecorated public methods with schema input/return annotations are also steps; `@step(...)` disambiguates bindings.
- `@transform(...)` accepts transform-level target and streaming options.

## General Step Operations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `project(...)` | `select` / projection | `return project(order, OrderPublished)(id=order.id)` |
| `where(...)` | `filter` / `where` | `where(order.total > 0)` |
| `@special(type="expr")` | Reusable `Column` expression | `@special(type="expr")\ndef clean(v): return trim(v)` |
| `compile_transform(...)` | Compiled DataFrame plan | `compiled = compile_transform(Publish)` |

**Details And Differences**

- `project(...)` builds typed projections; schema constructors and `Schema.project(...)` are often shorter.
- `where(...)` accepts symbolic Boolean expressions and can be chained with `.where(...)`.
- Expression specials compile without PySpark. Raw SQL and arbitrary Python UDF helpers remain outside the symbolic API.
- `compile_transform(...)` does not start a Spark job.

## Hooks And Diagnostics

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `@raw(...)` | Caller-owned DataFrame code | `@raw(inout=lane(clean) \| output(published))` |
| `SchemaMode` | Hook schema policy | `@raw(inout=lane(clean) \| output(published), schema_mode=SchemaMode.STRICT)` |
| `StructureCompileError` | Compile-time diagnostic | `except StructureCompileError as error: ...` |

**Details And Differences**

- `@raw(...)` is the explicit opaque boundary: Structure validates its binding declaration, not the hook body.
- `SchemaMode.STRICT` is the default; `SchemaMode.ALLOW_EXTRA_COLUMNS` permits additional hook output columns.
- `StructureCompileError` exposes a rendered diagnostic with remediation. See the [DSL reference](../reference/DSL.md).
