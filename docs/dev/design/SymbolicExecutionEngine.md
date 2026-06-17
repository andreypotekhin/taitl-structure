# Design: Symbolic Execution Engine

## Purpose

Symbolic execution turns user-written subtransform methods into compiler IR without executing real data processing.

## Core Idea

The compiler calls transform methods with symbolic schema row proxies.

```python
result = impl.normalize(symbolic_order)
```

Field access returns symbolic expressions.

```python
order.customer_id
```

becomes:

```text
FieldRef(scope="orders", path=["customer_id"], type=string)
```

## Recorded Operations

- field references
- literals
- expression calls
- filters from `where(...)`
- joins from `join_one(...)`
- schema object construction
- expression helper expansions

## Unsupported Operations

Unsupported operations raise structured compile errors. The engine should know the active transform, subtransform, and output field when possible.

## Flow

```text
StepPlan input schema
  ↓ create symbolic row proxy
execute user method
  ↓ collect filters, joins, projection
construct StepPlan
```

## Example

Source:

```python
where(order.id.is_not_null())
return OrderNormalized(
    id=order.id,
    customer_id=lower(trim(order.customer_id)),
)
```

IR:

```text
Filter: is_not_null(FieldRef(order.id))
Project:
  id <- FieldRef(order.id)
  customer_id <- lower(trim(FieldRef(order.customer_id)))
```

## Compile-Time Performance

Symbolic execution should be fast and side-effect free. Do not import Spark. Avoid AST parsing except for diagnostics. Cache expression helper expansion if safe.
