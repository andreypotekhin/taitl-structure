# Schema Inheritance

This specification completes the inheritance semantics referenced by
`docs/specifications/SchemaDeclarationSyntax.md`.

## Purpose

Schema inheritance lets developers reuse common field definitions without repeating schema code. It is especially useful
for shared identifiers, audit columns, partition columns, tenancy fields, and common source metadata.

Structure supports schema-to-schema inheritance in v1 as a declarative field composition mechanism. The compiler treats
the final schema as an ordered structural contract and keeps inheritance details available for diagnostics and lineage.

## Canonical Form

```python
from structure import Schema, field, String, Timestamp, Decimal


class EntityKeys(Schema):
    id = field(String(), nullable=False, primary_key=True)
    tenant_id = field(String(), nullable=False)


class AuditFields(Schema):
    created_at = field(Timestamp(), nullable=False)
    updated_at = field(Timestamp(), nullable=True)


class Order(EntityKeys, AuditFields):
    customer_id = field(String(), nullable=False)
    total = field(Decimal(12, 2), nullable=True)
```

Effective field order for `Order` is:

```text
id
tenant_id
created_at
updated_at
customer_id
total
```

## Supported Inheritance

Rules:

- A schema class may inherit from `Schema` directly.
- A schema class may inherit from one or more user-defined `Schema` subclasses.
- All non-`object` bases of a schema class must be `Schema` subclasses.
- Python must be able to construct a valid C3 MRO for the class.
- The compiler must reject base classes that are not import-safe.

Examples:

```python
class Customer(EntityKeys):
    name = field(String(), nullable=True)
```

```python
class Order(EntityKeys, AuditFields):
    total = field(Decimal(12, 2), nullable=True)
```

Non-schema mixins are not supported in v1:

```python
class Order(EntityKeys, SomePlainMixin):  # rejected
    total = field(Decimal(12, 2), nullable=True)
```

## Field Collection Algorithm

The compiler builds an effective ordered field map for each schema class.

Algorithm:

1. Start with an empty ordered map.
2. Visit direct schema bases from left to right as written in the class definition.
3. For each base, recursively collect that base's effective fields before collecting later bases.
4. Skip a schema base that was already visited through a diamond inheritance path.
5. Add local fields in class-body declaration order.
6. If a local field has the same name as an inherited field, replace the inherited field in the same position.
7. If a local field is new, append it after all inherited fields.

This gives users predictable field order: fields from the first base appear before fields from the second base, and
local fields appear last.

## Overrides

A schema class may override an inherited field by redeclaring the same field name.

```python
class SoftDeleteFields(Schema):
    deleted_at = field(Timestamp(), nullable=True)


class RequiredDeleteMarker(SoftDeleteFields):
    deleted_at = field(Timestamp(), nullable=False)
```

Override rules:

- Override replacement is whole-field replacement.
- Override position is the inherited field position.
- Type, nullability, primary key flag, metadata, and description all come from the overriding field.
- Metadata is not merged.
- Description is not merged.
- Overriding a field with a non-field value is rejected.
- Deleting an inherited field is not supported in v1.

Whole-field replacement keeps behavior simple and visible. A reader can inspect the overriding line and know the final
field definition.

## Duplicate Fields Across Bases

If two unrelated bases define the same field name, the subclass must redeclare that field locally to resolve the
ambiguity.

Rejected:

```python
class SourceKeys(Schema):
    id = field(String(), nullable=False)


class BusinessKeys(Schema):
    id = field(String(), nullable=False, primary_key=True)


class Order(SourceKeys, BusinessKeys):
    total = field(Decimal(12, 2), nullable=True)
```

Accepted:

```python
class Order(SourceKeys, BusinessKeys):
    id = field(String(), nullable=False, primary_key=True)
    total = field(Decimal(12, 2), nullable=True)
```

The resolved field keeps the first inherited position. In the accepted example, `id` remains before `total`.

Diamond inheritance through a shared base is not a duplicate:

```python
class Keys(Schema):
    id = field(String(), nullable=False)


class CustomerKeys(Keys):
    customer_id = field(String(), nullable=False)


class ProductKeys(Keys):
    product_id = field(String(), nullable=False)


class CustomerProduct(CustomerKeys, ProductKeys):
    score = field(Decimal(8, 4), nullable=True)
```

`id` is collected once because `Keys` is a shared ancestor.

## Field Origin

The schema model must retain field origin information.

For every effective field, `FieldDef` records:

- final owning schema;
- declaring schema;
- field name;
- effective order;
- whether it was inherited;
- whether it overrides another field;
- the overridden field origin when applicable.

This information is used for diagnostics, documentation, lineage, and source navigation.

## Schema Type Identity

Each schema class remains a distinct schema type even when it inherits all fields from another schema.

```python
class OrderRaw(EntityKeys):
    pass


class CustomerRaw(EntityKeys):
    pass
```

`OrderRaw` and `CustomerRaw` have compatible field structure but different schema identities. Transform flow validation
uses schema identity unless a compatibility rule explicitly asks for structural compatibility.

## Nested Structs

`Struct(SchemaClass)` uses the effective inherited field set of `SchemaClass`.

```python
class AddressBase(Schema):
    city = field(String(), nullable=True)


class ShippingAddress(AddressBase):
    postal_code = field(String(), nullable=True)


class Order(Schema):
    shipping = field(Struct(ShippingAddress), nullable=True)
```

Generated Spark schema for `shipping` includes both `city` and `postal_code`.

## Diagnostics

Inheritance diagnostics must include:

- schema class name;
- conflicting field name when applicable;
- base classes involved;
- source file and line when available;
- a concise fix.

Example:

```text
Ambiguous inherited field:
  Order.id is declared by SourceKeys and BusinessKeys.

Resolve the field in Order:
  class Order(SourceKeys, BusinessKeys):
      id = field(String(), nullable=False, primary_key=True)

See docs/specifications/SchemaInheritance.md
```

Example:

```text
Invalid schema base:
  Order inherits from SomePlainMixin, which is not a Schema subclass.

Use only Schema subclasses in schema inheritance.

See docs/specifications/SchemaInheritance.md
```

## Non-Goals

The following are not part of v1:

- deleting inherited fields;
- partial field overrides;
- metadata merging;
- description merging;
- non-schema mixins;
- changing field order locally without redeclaring the full schema;
- polymorphic transform dispatch based on schema subclassing.

## Implementation Checklist

1. Capture local field declaration order for every schema class.
2. Capture direct schema bases in class definition order.
3. Implement effective field collection with diamond de-duplication.
4. Detect unrelated base field collisions.
5. Require local overrides for unrelated base field collisions.
6. Preserve inherited field positions on override.
7. Store field origin and override metadata in `FieldDef`.
8. Use effective fields for Spark schema generation and runtime validation.
9. Add diagnostics that link to this specification.
10. Add tests for single inheritance, multiple inheritance, overrides, conflicts, and diamonds.

## Acceptance Criteria

- A schema can inherit fields from one schema base.
- A schema can inherit fields from multiple schema bases.
- Effective field order follows direct base order, then local declaration order.
- A subclass can override an inherited field, preserving the inherited position.
- Duplicate field names from unrelated bases are rejected unless the subclass resolves them locally.
- Diamond inheritance collects the shared ancestor's fields once.
- Generated Spark `StructType` uses the effective inherited field set.
- Field origin is available for diagnostics and generated documentation.
