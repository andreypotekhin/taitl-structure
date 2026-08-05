# Collections API

These helpers map to Spark array and map operations while keeping callback bodies symbolic. Examples abbreviate
`order.tags` as `o.tags` and `order.attributes` as `o.attributes`.

Typed struct generators are documented below. Raw or untyped PySpark generator escape hatches remain outside the
Structure contract.

## Simple Array Helpers

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `arr_flatten(...)` | `flatten` | `arr_flatten(order.nested_tags)` |
| `arr_distinct(...)` | `array_distinct` | `arr_distinct(order.tags)` |
| `arr_position(...)` | `array_position` | `arr_position(order.tags, "priority")` |
| `size(...)` | `size` | `size(order.tags)` |
| `array_contains(...)` | `array_contains` | `array_contains(order.tags, "priority")` |
| `array(...)` | `array` | `array("priority", "standard")` |
| `array_repeat(...)` | `array_repeat` | `array_repeat("priority", 2)` |
| `array_union(...)` | `array_union` | `array_union(order.tags, order.extra_tags)` |
| `array_except(...)` | `array_except` | `array_except(order.tags, order.extra_tags)` |
| `array_intersect(...)` | `array_intersect` | `array_intersect(order.tags, order.extra_tags)` |
| `slice(...)` | `slice` | `slice(order.tags, 1, 10)` |
| `sequence(...)` | `sequence` | `sequence(order.first, order.last, step=1)` |
| `arr_append(...)`, `arr_prepend(...)` | Array mutation | `arr_append(order.tags, "priority")` |
| `arr_insert(...)`, `arr_remove(...)` | Array mutation | `arr_insert(order.tags, 1, "priority")` |
| `arr_compact(...)` | `array_compact` | `arr_compact(order.tags)` |
| `element_at(...)` | `element_at` | `element_at(order.tags, 1)` |
| `try_element_at(...)` | `try_element_at` | `try_element_at(order.tags, 2)` |

**Details And Differences**

- `array(...)` needs at least one typed value. Compatible numeric values widen from Integer to Long to Float to Double;
  other element types must match.
- `arr_position(...)` requires a compatible Python literal as its searched item. Column items are unavailable in the
  supported PySpark 3.5 baseline.
- `map_contains_key(...)` likewise requires a compatible Python literal key in the supported PySpark 3.5 baseline.
- Array indices are one-based. `element_at(...)` follows Spark's ANSI out-of-range behavior, while
  `try_element_at(...)` yields null for a missing or out-of-range element.
- `slice(...)` uses Spark's one-based start position and rejects a negative length before compilation.
- `sequence(...)` supports compatible Integer or Long bounds and an optional nonzero step.
- `arr_append(...)`, `arr_prepend(...)`, and `arr_insert(...)` preserve typed array contents; insertion positions are
  one-based. `arr_remove(...)` deliberately requires a non-null Python literal for the PySpark 3.5 baseline.
- `arr_compact(...)` removes null items, so its result has `contains_null=False`.
- `arr_flatten(...)` yields null when the outer array is null or contains a null immediate nested array, matching
  Spark's `flatten` behavior.
- Lookup results are nullable because a map key can be absent and a safe array lookup can be out of range.

## Array Callbacks

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `arr_transform(...)` | `transform` | `arr_transform(order.tags, lambda tag: lower(tag))` |
| `arr_filter(...)` | `filter` | `arr_filter(order.tags, lambda tag: tag.is_not_null())` |
| `arr_exists(...)` | `exists` | `arr_exists(order.tags, lambda tag: tag == "priority")` |
| `arr_forall(...)` | `forall` | `arr_forall(order.tags, lambda tag: tag.is_not_null())` |
| `arr_zip_with(...)` | `zip_with` | `arr_zip_with(order.tags, order.tags, lambda left, right: left)` |
| `arr_aggregate(...)` | `aggregate` | `arr_aggregate(order.scores, 0, lambda acc, score: acc + score)` |
| `arr_sort(...)` | `array_sort` | `arr_sort(order.tags)` |
| `arr_sort_by(...)` | `array_sort` | `arr_sort_by(order.tags, lambda tag: tag, descending=True)` |
| `arr_reverse(...)` | `reverse` | `arr_reverse(order.tags)` |

**Details And Differences**

- Callbacks run once during symbolic compilation, not once per Python row.
- Predicate callbacks must return symbolic Boolean expressions; merge and sort callbacks must return symbolic values.
- `arr_exists(...)` and `arr_forall(...)` can yield null under Spark's three-valued predicate semantics when an item or
  predicate result is null and no decisive true/false result is present.
- `arr_aggregate(...)` yields null for a null input array. Without `finish=`, an empty array returns `initial` unchanged,
  so a nullable initial accumulator also makes the result nullable.
- `arr_aggregate(...)` merge callbacks must return exactly the initial accumulator type; `finish=` may convert that
  accumulated value to a different final type.
- `map_zip_with(...)` requires identical map key types; it does not apply numeric key widening.
- `arr_sort_by(..., descending=...)` requires a Boolean direction flag.
- `arr_sort(...)` accepts arrays whose element type Spark can order; `arr_reverse(...)` preserves the array element type.

## Typed Struct Generators

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `explode_struct(...)` | `explode` | `item = explode_struct(order.items, as_=OrderItem)` |
| `explode_outer_struct(...)` | `explode_outer` | `item = explode_outer_struct(order.items, as_=OrderItem)` |
| `posexplode_struct(...)` | `posexplode` | `item = posexplode_struct(order.items, as_=PositionedItem)` |
| `posexplode_outer_struct(...)` | `posexplode_outer` | `item = posexplode_outer_struct(order.items, as_=PositionedItem)` |
| `inline_struct(...)` | `inline` | `item = inline_struct(order.items, as_=OrderItem)` |
| `inline_outer_struct(...)` | `inline_outer` | `item = inline_outer_struct(order.items, as_=OrderItem)` |

**Details And Differences**

- Each generator requires an `array<struct>` expression and an explicit `as_` Schema. `scope=` controls the generated
  row scope used by later expressions.
- Inner generators drop null or empty arrays. Outer generators preserve the input row with nullable generated fields.
- `posexplode_struct(...)` and its outer form add a zero-based ordinal field, named `ordinal` by default.
- Generator cardinality and streaming compatibility are recorded in the compiler plan; generated fields remain typed and
  compiler-visible.

## Map Helpers

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `map_keys(...)` | `map_keys` | `map_keys(order.attributes)` |
| `map_values(...)` | `map_values` | `map_values(order.attributes)` |
| `map_entries(...)` | `map_entries` | `map_entries(order.attributes)` |
| `map_from_entries(...)` | `map_from_entries` | `map_from_entries(map_entries(order.attributes))` |
| `map_contains_key(...)` | `map_contains_key` | `map_contains_key(order.attributes, "region")` |
| `map_concat(...)` | `map_concat` | `map_concat(order.attributes, order.extra_attributes)` |
| `element_at(...)` | `element_at` | `element_at(order.attributes, "region")` |
| `try_element_at(...)` | `try_element_at` | `try_element_at(order.attributes, "region")` |
| `map_transform_values(...)` | `transform_values` | `map_transform_values(o.attributes, lambda k, v: lower(v))` |
| `map_transform_keys(...)` | `transform_keys` | `map_transform_keys(order.attributes, lambda key, value: lower(key))` |
| `map_filter(...)` | `map_filter` | `map_filter(order.attributes, lambda key, value: value.is_not_null())` |
| `map_zip_with(...)` | `map_zip_with` | `map_zip_with(o.left, o.right, lambda k, l, r: coalesce(l, r))` |

**Details And Differences**

- Transform and filter callbacks receive symbolic key/value expressions.
- Duplicate transformed keys are rejected by Structure's strict map contract.
- Map keys cannot contain maps, including through an array or struct key shape, matching Spark's map-key domain.
- `map_concat(...)` requires matching key and value types; it does not apply numeric widening between maps.
- `map_concat(...)` accepts `duplicates="error"` only. Its inputs must not contain duplicate runtime keys; run Spark
  with `spark.sql.mapKeyDedupPolicy=EXCEPTION` (the default) so a conflicting merge fails instead of silently choosing
  a value.
- Python callback control flow and raw/untyped row-expanding generators such as direct `explode(...)` are unsupported.
  Use the typed struct generator forms above when the element schema is known. See the
  [Transforms background](../background/Transform.back.md).
