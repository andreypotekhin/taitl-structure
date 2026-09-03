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

## Array SQL Helpers

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `concat(...)` | `concat` | `concat(order.tags, order.extra_tags)` |
| `cardinality(...)` | `cardinality` | `cardinality(order.tags)` or `cardinality(order.attributes)` |
| `array_size(...)` | `array_size` | `array_size(order.tags)` |
| `array_join(...)` | `array_join` | `array_join(order.tags, ",", "<null>")` |
| `array_max(...)`, `array_min(...)` | `array_max`, `array_min` | `array_max(order.scores)` |
| `arrays_overlap(...)` | `arrays_overlap` | `arrays_overlap(order.tags, order.extra_tags)` |
| `get(...)` | `get` | `get(order.tags, 0)` |
| `sort_array(...)` | `sort_array` | `sort_array(order.scores, ascending=False)` |
| `shuffle(...)` | `shuffle` | `shuffle(order.tags)` |

**Details And Differences**

- `concat(...)` accepts at least two homogeneous string, binary, or array values. Array elements may use the existing
  compatible numeric widening rules; mixed families are rejected before lowering. The result preserves source
  nullability and, for arrays, propagates element nullability.
- `cardinality(...)` accepts arrays and maps; `array_size(...)` accepts arrays only. Both return nullable Integer values
  when their collection input is nullable.
- `array_join(...)` requires `array<string>` and literal string delimiter/replacement arguments. Its result follows the
  source-array nullability.
- `array_max(...)` and `array_min(...)` require orderable scalar elements and return a nullable element value, including
  for empty arrays or arrays that contain nulls.
- `arrays_overlap(...)` requires compatible array element types and is nullable when either array or either element
  domain can be null.
- `get(...)` uses Spark's zero-based array index. It returns a nullable element for out-of-range or null input; use
  `element_at(...)` or `try_element_at(...)` for one-based lookup semantics.
- `sort_array(...)` requires orderable scalar elements and a Boolean `ascending` flag; it preserves the source array
  type and nullability.
- `shuffle(...)` preserves the source array type and nullability but is nondeterministic; callers must not depend on
  the returned element order, including across retries, repartitioning, or query restarts.

## Array Callbacks

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `arr_transform(...)` | `transform` | `arr_transform(order.tags, lambda tag: lower(tag))` or `arr_transform(order.values, lambda value, index: value + index)` |
| `arr_filter(...)` | `filter` | `arr_filter(order.tags, lambda tag: tag.is_not_null())` or `arr_filter(order.values, lambda value, index: index % 2 == 0)` |
| `arr_exists(...)` | `exists` | `arr_exists(order.tags, lambda tag: tag == "priority")` |
| `arr_forall(...)` | `forall` | `arr_forall(order.tags, lambda tag: tag.is_not_null())` |
| `arr_zip_with(...)` | `zip_with` | `arr_zip_with(order.tags, order.tags, lambda left, right: left)` |
| `arrays_zip(...)` | `arrays_zip` | `arrays_zip(order.tags, order.priorities)` |
| `arr_aggregate(...)` | `aggregate` | `arr_aggregate(order.scores, 0, lambda acc, score: acc + score)` |
| `reduce(...)` | `reduce` | `reduce(order.scores, 0, lambda acc, score: acc + score)` |
| `arr_sort(...)` | `array_sort` | `arr_sort(order.tags)` |
| `arr_sort_by(...)` | `array_sort` | `arr_sort_by(order.tags, lambda tag: tag, descending=True)` |
| `arr_reverse(...)` | `reverse` | `arr_reverse(order.tags)` |

**Details And Differences**

- Callbacks run once during symbolic compilation, not once per Python row.
- `arr_transform(...)` and `arr_filter(...)` accept unary `(item)` or binary `(item, index)` callbacks. The binary index is
  zero-based, non-null, and typed as `long`; for filtering it always refers to the original input-array position.
- An indexed transform can return a declared struct to produce an ordinal-aware nested output without exploding rows:
  `arr_transform(order.tags, lambda tag, index: PositionedTag(value=tag, ordinal=index))`, where `PositionedTag` is a
  declared `Schema` with `value` and `ordinal` fields.
- Predicate callbacks must return symbolic Boolean expressions; merge and sort callbacks must return symbolic values.
- `arr_exists(...)` and `arr_forall(...)` can yield null under Spark's three-valued predicate semantics when an item or
  predicate result is null and no decisive true/false result is present.
- `arr_aggregate(...)` yields null for a null input array. Without `finish=`, an empty array returns `initial` unchanged,
  so a nullable initial accumulator also makes the result nullable.
- `arr_aggregate(...)` merge callbacks must return exactly the initial accumulator type; `finish=` may convert that
  accumulated value to a different final type.
- `reduce(...)` has the same typed accumulator and optional finish contract, but renders the exact PySpark `reduce`
  spelling. Its merge callback must return exactly the initial accumulator type.
- `arrays_zip(...)` accepts one or more arrays and returns an array of structs with stable nullable fields named
  `array_0`, `array_1`, and so on. The result is nullable when any input array is nullable; padded fields are nullable
  because Spark fills shorter arrays with null.
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

## Typed Scalar-Array Generators

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `explode_array(...)` | `explode` | `item = explode_array(document.lines, as_=Line, value_field="line")` |
| `explode_outer_array(...)` | `explode_outer` | `item = explode_outer_array(document.lines, as_=NullableLine, value_field="line")` |
| `posexplode_array(...)` | `posexplode` | `item = posexplode_array(document.lines, as_=PositionedLine, value_field="line")` |
| `posexplode_outer_array(...)` | `posexplode_outer` | `item = posexplode_outer_array(document.lines, as_=NullablePositionedLine, value_field="line")` |

Scalar-array generators accept primitive arrays only: strings, booleans, numeric values, dates, timestamps, and binary
values. `as_` must declare exactly the named `value_field`; positional forms also declare a long `ordinal` field.
Inner forms require a non-null array with non-null elements. Outer forms preserve Spark's null/empty input row and
require nullable generated fields. Nested arrays, maps, structs, variants, and indexed callbacks are not admitted.

## Typed Map Generators

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `explode_map(...)` | `explode` | `entry = explode_map(document.attributes, as_=Entry, key_field="key", value_field="value")` |
| `explode_outer_map(...)` | `explode_outer` | `entry = explode_outer_map(document.attributes, as_=OuterEntry, key_field="key", value_field="value")` |
| `posexplode_map(...)` | `posexplode` | `entry = posexplode_map(document.attributes, as_=PositionedEntry, key_field="key", value_field="value")` |
| `posexplode_outer_map(...)` | `posexplode_outer` | `entry = posexplode_outer_map(document.attributes, as_=OuterPositionedEntry, key_field="key", value_field="value")` |

Map generators admit primitive scalar keys and values. The generated Schema must declare the explicit `key_field` and
`value_field`; positional forms additionally declare a long `ordinal`. Inner forms require a non-null map expression,
non-null keys, and preserve nullable map values. Outer forms make key, value, and ordinal fields nullable so null or
empty maps preserve their source row. Nested maps, structs, variants, and compatibility aliases remain deferred.

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
  Use the typed struct or scalar-array generator forms above when the element shape is admitted. See the
  [Transforms background](../background/Transform.back.md).
