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
- schema base overlay construction
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

Schema base overlays are shorthand for the same projection shape. A transform may extend an inherited schema row by
copying the inherited fields from a symbolic row and naming only new or changed fields:

```python
return OrderWithCustomer.base(order)(
    customer_name=customer.name,
    customer_tier=customer.tier,
)
```

IR:

```text
Project:
  tenant <- FieldRef(order.tenant)
  ...
  customer_name <- FieldRef(customer.name)
  customer_tier <- FieldRef(customer.tier)
```

When a target schema has multiple direct schema bases, `base(...)` receives one source row per direct base, in schema
declaration order. The symbolic engine assigns fields by inherited field origin rather than by fuzzy name matching.
Local target fields and locally overridden inherited fields must be explicit in the overlay call.

```python
flags = PublicationFlags(
    has_promotion=order.promotion_name.is_not_null(),
)

return OrderPublished.base(order, flags)
```

IR:

```text
Project:
  id <- FieldRef(order.id)
  ...
  has_promotion <- is_not_null(FieldRef(order.promotion_name))
```

## Compile-Time Performance

Symbolic execution should be fast and side-effect free. Do not import Spark. Avoid AST parsing except for diagnostics. Cache expression helper expansion if safe.
