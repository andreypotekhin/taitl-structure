# Collections Compatibility

This is the compatibility companion to the [API reference](../api/Collections.api.md). It records Structure contracts alongside the corresponding PySpark API forms and examples for read-through. The shared baseline is the public PySpark 3.5.x/4.0.x intersection; Connect is claimed only where runtime evidence is recorded.

## Array Lookup and Size Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `size(...)` | `size` | `size(order.tags)` | yes | yes | Accepts Array or Map and returns nullable Integer. For null input, Spark's value depends on `spark.sql.legacy.sizeOfNull` and ANSI configuration. |
| `cardinality(...)` | `cardinality` | `cardinality(order.tags)` or `cardinality(order.attributes)` | yes | yes | Accepts Array or Map and returns nullable Integer; Spark owns its documented legacy/ANSI null policy. |
| `array_size(...)` | `array_size` | `array_size(order.tags)` | yes | yes | Accepts Array only and returns nullable Integer; null input remains null. |
| `array_contains(...)` | `array_contains` | `array_contains(order.tags, "priority")` | yes | yes | Requires compatible typed values and preserves Spark's nullable Boolean result for null arrays/elements. |
| `arr_position(...)` | `array_position` | `arr_position(order.tags, "priority")` | yes | yes | Requires a PySpark-3.5-compatible literal item and returns the first one-based position as nullable Long (zero means absent). |
| `get(...)` | `get` | `get(order.tags, 0)` | yes | yes | Accepts an integral index expression and uses zero-based indexing; out-of-range access returns null. |
| `element_at(...)` | `element_at` | `element_at(order.attributes, "region")` | yes | yes | Accepts Array/Map. Array indexes are one-based (negative indexes count from the end); zero is invalid, and out-of-range behavior follows Spark ANSI mode. Map misses return null. |
| `try_element_at(...)` | `try_element_at` | `try_element_at(order.attributes, "region")` | yes | yes | Has the same typed Array/Map contract but returns null for missing/out-of-range lookups. Structure treats Python strings as literals; use a typed field expression when migrating PySpark's string-as-column-name form. |
| `slice(...)` | `slice` | `slice(order.tags, 1, 10)` | yes | yes | Accepts integral expressions, uses one-based starts (negative starts count from the end), and rejects negative literal lengths. |

## Array Construction and Mutation Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `array(...)` | `array` | `array("priority", "standard")` | yes | yes | Requires at least one typed value, unifies compatible element types, and derives element nullability. |
| `array_repeat(...)` | `array_repeat` | `array_repeat("priority", 2)` | yes | yes | Requires an integral count expression and returns an Array with the input element type. |
| `sequence(...)` | `sequence` | `sequence(order.first, order.last, step=1)` | yes | yes | Status: `caller-owned-guided` for Date/Timestamp ranges. The Structure helper admits compatible Integer/Long expressions and rejects a zero literal step. Migration: Use native PySpark for Date/Timestamp sequences. |
| `arr_append(...)` | `array_append` | `arr_append(order.tags, "priority")` | yes | yes | Appends one compatible typed item and carries element nullability. |
| `arr_prepend(...)` | `array_prepend` | `arr_prepend(order.tags, "priority")` | yes | yes | Prepends one compatible typed item and carries element nullability. |
| `arr_insert(...)` | `array_insert` | `arr_insert(order.tags, 1, "priority")` | yes | yes | Requires a nonzero integral Python literal position; positive positions are one-based and negative positions count from the end. |
| `arr_remove(...)` | `array_remove` | `arr_remove(order.tags, "priority")` | yes | yes | Status: `caller-owned-guided` for row-valued removal items. Requires a non-null compatible Python literal for the shared 3.5 baseline. Migration: Use `arr_remove(...)` for literal items; use native PySpark for row-valued removal items. |
| `arr_compact(...)` | `array_compact` | `arr_compact(order.tags)` | yes | yes | Removes null elements and narrows the result to `contains_null=False`. |

## Array Composition and Ordering Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `concat(...)` | `concat` | `concat(order.tags, order.extra_tags)` | yes | yes | Accepts two or more homogeneous String, Binary, or compatible Array expressions. |
| `array_join(...)` | `array_join` | `array_join(order.tags, ",", "<null>")` | yes | yes | Accepts Array[String] and literal String options; nullability follows the input array. |
| `array_max(...)` | `array_max` | `array_max(order.scores)` | yes | yes | Returns the nullable maximum orderable scalar element, ignoring null elements. |
| `array_min(...)` | `array_min` | `array_min(order.scores)` | yes | yes | Returns the nullable minimum orderable scalar element, ignoring null elements. |
| `arrays_overlap(...)` | `arrays_overlap` | `arrays_overlap(order.tags, order.extra_tags)` | yes | yes | Requires compatible element types and preserves Spark's three-valued null semantics. |
| `sort_array(...)` | `sort_array` | `sort_array(order.scores, ascending=False)` | yes | yes | Accepts orderable scalar elements and a Boolean literal direction; null ordering follows Spark. |
| `shuffle(...)` | `shuffle` | `shuffle(order.tags)` | yes | yes | Preserves the array type and is marked nondeterministic. |

## Array Transform and Set Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `arr_distinct(...)` | `array_distinct` | `arr_distinct(order.tags)` | yes | yes | Removes duplicate elements while preserving the typed array. |
| `array_union(...)` | `array_union` | `array_union(order.tags, order.extra_tags)` | yes | yes | Returns the union of compatible typed arrays with Spark's array semantics. |
| `array_intersect(...)` | `array_intersect` | `array_intersect(order.tags, order.extra_tags)` | yes | yes | Returns the common elements of compatible typed arrays. |
| `array_except(...)` | `array_except` | `array_except(order.tags, order.extra_tags)` | yes | yes | Returns left-side elements absent from the compatible right array. |
| `arr_sort(...)` | `array_sort` | `arr_sort(order.tags)` | yes | yes | Status: `caller-owned-guided` for arbitrary PySpark comparator callbacks. Provides Spark's default ascending order; `arr_sort_by(value, key)` orders by a typed scalar key. Migration: Use `arr_sort(...)` or `arr_sort_by(...)` for the admitted forms; retain native PySpark for arbitrary comparator logic. |
| `reverse(...)` | `reverse` | `reverse(order.name)` | yes | yes | Handles String expressions and `arr_reverse(value)` handles Array expressions. |
| `arr_flatten(...)` | `flatten` | `arr_flatten(order.nested_tags)` | yes | yes | Flattens one level of a typed nested array. |
| `arr_transform(...)` | `transform` | Array-element transform: `arr_transform(order.tags, lambda tag: lower(tag))` or `arr_transform(order.values, lambda value, index: value + index)` | yes | yes | Accepts symbolic element or element/index callbacks; the index is zero-based. |
| `arr_filter(...)` | `filter` | `arr_filter(order.tags, lambda tag: tag.is_not_null())` or `arr_filter(order.values, lambda value, index: index % 2 == 0)` | yes | yes | Accepts symbolic Boolean element or element/index predicates; the optional index is zero-based. |
| `arr_exists(...)` | `exists` | `arr_exists(order.tags, lambda tag: tag == "priority")` | yes | yes | Accepts a typed Boolean element predicate and preserves nullable Boolean semantics. |
| `arr_forall(...)` | `forall` | `arr_forall(order.tags, lambda tag: tag.is_not_null())` | yes | yes | Accepts a typed Boolean element predicate and preserves nullable Boolean semantics. |
| `arr_aggregate(...)` | `aggregate` | `arr_aggregate(order.scores, 0, lambda acc, score: acc + score)` | yes | yes | Requires a type-stable accumulator and typed merge/finish callbacks. |
| `reduce(...)` | `reduce` | `reduce(order.scores, 0, lambda acc, score: acc + score)` | yes | yes | Preserves the PySpark spelling and the typed accumulator contract. |
| `arr_zip_with(...)` | `zip_with` | `arr_zip_with(order.tags, order.tags, lambda left, right: left)` | yes | yes | Types both callback arguments as nullable because Spark pads the shorter array with nulls. |
| `arrays_zip(...)` | `arrays_zip` | `arrays_zip(order.tags, order.priorities)` | yes | yes | Status: `caller-owned-guided` when downstream code depends on Spark-derived field names. Returns a typed Array[Struct] with compiler-visible fields `array_0`, `array_1`, ... . Migration: Use the positional typed fields when sufficient; use native PySpark when Spark-derived field names are required. |
| `arr_sort_by(...)` | `array_sort` | `arr_sort_by(order.tags, lambda tag: tag, descending=True)` | yes | yes | Orders array elements by a symbolic scalar key. Status: `caller-owned-guided` for arbitrary PySpark comparator callbacks. |
| `arr_reverse(...)` | `reverse` | `arr_reverse(order.tags)` | yes | yes | Reverses a typed Array; `reverse()` handles String expressions. |

## Generator Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `stack(...)` | `stack` | `stack(2, order.id, order.name, order.id, as_=StackRow)` | yes | yes | Fixes row multiplication, position-wise common types, and trailing-NULL padding while keeping output schema explicit. Preserve output aliases, types, nullability, and streaming row expansion. |
| `explode_struct(...)` | `explode` | `explode_struct(order.items, as_=Item)` | yes | yes | Array<struct> input; fields are declared by `as_`. The result schema is declared explicitly. |
| `explode_array(...)` | `explode` | `explode_array(order.attributes, as_=MapEntry)` | yes | yes | Primitive scalar-array input; the element field is declared by `as_`. The result schema is declared explicitly. |
| `explode_map(...)` | `explode` | `explode_map(order.attributes, as_=MapEntry)` | yes | yes | Primitive scalar-map input; key/value fields are declared by `as_`. The result schema is declared explicitly. |
| `explode_outer_struct(...)` | `explode_outer` | `explode_outer_struct(order.items, as_=Item)` | yes | yes | Array<struct> input; preserves null/empty rows and requires nullable output fields. The result schema is declared explicitly. |
| `explode_outer_array(...)` | `explode_outer` | `explode_outer_array(order.attributes, as_=MapEntry)` | yes | yes | Primitive scalar-array input; preserves null/empty rows and requires nullable output fields. The result schema is declared explicitly. |
| `explode_outer_map(...)` | `explode_outer` | `explode_outer_map(order.attributes, as_=MapEntry)` | yes | yes | Primitive scalar-map input; preserves null/empty rows and requires nullable output fields. The result schema is declared explicitly. |
| `posexplode_struct(...)` | `posexplode` | `posexplode_struct(order.items, as_=Item)` | yes | yes | Array<struct> input; adds a zero-based Long ordinal and declared fields. The result schema is declared explicitly. |
| `posexplode_array(...)` | `posexplode` | `posexplode_array(order.attributes, as_=MapEntry)` | yes | yes | Primitive scalar-array input; adds a zero-based Long ordinal. The result schema is declared explicitly. |
| `posexplode_map(...)` | `posexplode` | `posexplode_map(order.attributes, as_=MapEntry)` | yes | yes | Primitive scalar-map input; adds a zero-based Long ordinal and key/value fields. The result schema is declared explicitly. |
| `posexplode_outer_struct(...)` | `posexplode_outer` | `posexplode_outer_struct(order.items, as_=Item)` | yes | yes | Array<struct> input; adds a nullable ordinal and preserves null/empty rows. The result schema is declared explicitly. |
| `posexplode_outer_array(...)` | `posexplode_outer` | `posexplode_outer_array(order.attributes, as_=MapEntry)` | yes | yes | Primitive scalar-array input; adds a nullable ordinal and preserves null/empty rows. The result schema is declared explicitly. |
| `posexplode_outer_map(...)` | `posexplode_outer` | `posexplode_outer_map(order.attributes, as_=MapEntry)` | yes | yes | Primitive scalar-map input; adds a nullable ordinal and preserves null/empty rows. The result schema is declared explicitly. |
| `inline_struct(...)` | `inline` | `inline_struct(order.items, as_=Item)` | yes | yes | `inline_struct`; inlines declared fields from an `array<struct>` into the generated scope. |
| `inline_outer_struct(...)` | `inline_outer` | `inline_outer_struct(order.items, as_=Item)` | yes | yes | `inline_outer_struct`; inlines `array<struct>` fields while retaining null/empty input rows. |

## Map and Struct Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `create_map(...)` | `create_map` | `create_map("region", order.region, "tier", order.tier)` | yes | yes | Requires alternating typed key/value inputs and non-null keys. |
| `map_from_arrays(...)` | `map_from_arrays` | `map_from_arrays(array("region"), array(order.region))` | yes | yes | Typed arrays require compatible element families and non-null keys. |
| `str_to_map(...)` | `str_to_map` | `str_to_map(order.attributes_text, ";", "=")` | yes | yes | Delimiters are non-empty String literals. |
| `named_struct(...)` | `named_struct` | `named_struct("region", order.region, "tier", order.tier)` | yes | yes | Field names are unique non-empty String literals and values preserve declared types/nullability. |
| `map_from_entries(...)` | `map_from_entries` | `map_from_entries(map_entries(order.attributes))` | yes | yes | Requires an array of two-field key/value structs with non-null keys. |
| `map_keys(...)` | `map_keys` | `map_keys(order.attributes)` | yes | yes | Returns a typed array of the map key type. |
| `map_values(...)` | `map_values` | `map_values(order.attributes)` | yes | yes | Returns a typed array preserving value element nullability. |
| `map_entries(...)` | `map_entries` | `map_entries(order.attributes)` | yes | yes | Returns a typed array of key/value structs. |
| `map_contains_key(...)` | `map_contains_key` | `map_contains_key(order.attributes, "region")` | yes | yes | Returns nullable Boolean and validates key compatibility. |
| `map_concat(...)` | `map_concat` | `map_concat(order.attributes, order.extra_attributes)` | yes | yes | Requires compatible key/value types and rejects duplicate runtime keys under Spark's exception policy. |
| `map_filter(...)` | `map_filter` | `map_filter(order.attributes, lambda key, value: value.is_not_null())` | yes | yes | Accepts a symbolic Boolean predicate. |
| `map_transform_keys(...)` | `transform_keys` | `map_transform_keys(order.attributes, lambda key, value: lower(key))` | yes | yes | Returns a typed map with the callback result key type. |
| `map_transform_values(...)` | `transform_values` | `map_transform_values(order.attributes, lambda key, value: lower(value))` | yes | yes | Returns a typed map with the callback result value type. |
| `map_zip_with(...)` | `map_zip_with` | `map_zip_with(order.left, order.right, lambda key, left, right: coalesce(left, right))` | yes | yes | Requires matching key types and returns typed merged values. |

## Caller-owned generator boundary

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| — | Generic generators | — | yes | yes | Status: `caller-owned-guided`. Only `stack` and the typed collection generators have fixed result contracts; raw generators lack a static output schema and cardinality. Migration: Keep arbitrary generators at a native PySpark boundary with an explicitly declared result Schema. |

