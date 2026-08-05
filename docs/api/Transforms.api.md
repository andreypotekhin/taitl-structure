# Transforms API

These declarations and operations define compiler-visible transform methods. Examples assume declared `OrderRaw`,
`OrderClean`, `OrderPublished`, and `order` schemas or row scopes.

Whole-rowset operations such as set composition, ordering, assertions, hierarchy expansion, sampling, and bounded scans
are documented in the [Relations API](Relations.api.md).

## Simple Transform Declarations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `Transform` | DataFrame pipeline class | `class Publish(Transform): pass` |
| `input(...)` | DataFrame input | `orders = input(OrderRaw)` |
| `output(...)` | DataFrame result | `published = output(OrderPublished)` |
| `lane(...)` | Intermediate DataFrame | `clean = lane(OrderClean)` |
| `stage(...)` | Explicit composed-stage compatibility API | `normalized = NormalizeOrders(orders=orders)` |
| `@transform(...)` | Pipeline declaration | `@transform\nclass Publish(Transform): pass` |
| `@step(...)` | Named pipeline step | `@step(inout=lane(clean) \| output(published))` |

**Details And Differences**

- `input(...)`, `output(...)`, and `lane(...)` declare named transform boundaries. A graph may collect explicit output
  sources with `outputs = output(name=stage.output, ...)` while keeping schemas declared separately.
- In a class-body stage graph, `stage(...)` is optional: assigning a transform invocation directly, such as
  `normalized = NormalizeOrders(orders=orders)`, declares the assignment as a stage. Direct assignments can be chained
  with `normalized.output`; ordinary Python assignments are ignored. The explicit `stage(...)` form remains supported.
- Undecorated public methods with schema input/return annotations are also steps; `@step(...)` disambiguates bindings.
- `@transform(...)` accepts transform-level target and streaming options.

## General Step Operations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `project(...)` | `select` / projection | `return project(order, OrderPublished)(id=order.id)` |
| `where(...)` | `filter` / `where` | `where(order.total > 0)` |
| `@step(cache=True)` | `persist()` | `@step(cache=True)` |
| `@step(cache=StorageLevel.MEMORY_AND_DISK)` | `persist(StorageLevel.MEMORY_AND_DISK)` | `@step(cache=StorageLevel.MEMORY_AND_DISK)` |
| `@special(type="expr")` | Reusable `Column` expression | `@special(type="expr")\ndef clean(v): return trim(v)` |
| `Compiler.frontend.analyze()` | Structural transform plan | `plan = Compiler.frontend.analyze()(Publish)` |
| `Compiler.frontend.compile()` | Selected-platform compilation | `compiled = Compiler.frontend.compile()(Publish)` |

**Details And Differences**

- `project(...)` builds typed projections; schema constructors and `Schema.project(...)` are often shorter.
- `where(...)` accepts symbolic Boolean expressions and can be chained with `.where(...)`.
- `cache=True` persists the completed step at PySpark's default storage level. Supply a PySpark `StorageLevel` for an
  explicit level; Structure preserves its disk, memory, off-heap, deserialization, and replication settings in both
  generated and online execution.
- Expression specials compile without PySpark. Raw SQL and arbitrary Python UDF helpers remain outside the symbolic API.
- `Compiler.frontend.analyze()` does not invoke step methods or start a Spark job.
- `Compiler.frontend.compile()` authors and compiles for the selected platform but does not start a Spark job.

## Hooks And Diagnostics

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `@raw(...)` | Caller-owned DataFrame code | `@raw(inout=lane(clean) \| output(published))` |
| `SchemaMode` | Hook schema policy | `@raw(inout=lane(clean) \| output(published), schema_mode=SchemaMode.STRICT)` |
| `StructureCompileError` | Compile-time diagnostic | `except StructureCompileError as error: ...` |

**Details And Differences**

- `@raw(...)` is the explicit opaque boundary: Structure validates its binding declaration, not the hook body.
- `SchemaMode.STRICT` is the default; `SchemaMode.ALLOW_EXTRA_COLUMNS` permits additional hook output columns.
- `StructureCompileError` exposes a rendered diagnostic with remediation. See the
  [Transforms background](../background/Transform.back.md) and [Hooks reference](../background/HookSemantics.back.md).
