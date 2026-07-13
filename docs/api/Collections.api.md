# Collections API

These helpers map to Spark array and map operations while keeping callback bodies symbolic. Examples abbreviate
`order.tags` as `o.tags` and `order.attributes` as `o.attributes`.

## Simple Array Helpers

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `arr_flatten(...)` | `flatten` | `arr_flatten(order.nested_tags)` |
| `arr_distinct(...)` | `array_distinct` | `arr_distinct(order.tags)` |
| `arr_position(...)` | `array_position` | `arr_position(order.tags, "priority")` |

**Details And Differences**

- These helpers preserve Structure array type and nullability metadata.
- Size, membership, construction, set operations, and safe element lookup are planned for Sprint 15.

## Array Callbacks

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `arr_transform(...)` | `transform` | `arr_transform(order.tags, lambda tag: lower(tag))` |
| `arr_filter(...)` | `filter` | `arr_filter(order.tags, lambda tag: tag.is_not_null())` |
| `arr_exists(...)` | `exists` | `arr_exists(order.tags, lambda tag: tag == "priority")` |
| `arr_forall(...)` | `forall` | `arr_forall(order.tags, lambda tag: tag.is_not_null())` |
| `arr_zip_with(...)` | `zip_with` | `arr_zip_with(order.tags, order.tags, lambda left, right: left)` |
| `arr_aggregate(...)` | `aggregate` | `arr_aggregate(order.scores, 0, lambda acc, score: acc + score)` |
| `arr_sort_by(...)` | `array_sort` | `arr_sort_by(order.tags, lambda tag: tag, descending=True)` |

**Details And Differences**

- Callbacks run once during symbolic compilation, not once per Python row.
- Predicate callbacks must return symbolic Boolean expressions; merge and sort callbacks must return symbolic values.

## Map Helpers

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `map_keys(...)` | `map_keys` | `map_keys(order.attributes)` |
| `map_values(...)` | `map_values` | `map_values(order.attributes)` |
| `map_entries(...)` | `map_entries` | `map_entries(order.attributes)` |
| `map_from_entries(...)` | `map_from_entries` | `map_from_entries(map_entries(order.attributes))` |
| `map_transform_values(...)` | `transform_values` | `map_transform_values(o.attributes, lambda k, v: lower(v))` |
| `map_transform_keys(...)` | `transform_keys` | `map_transform_keys(order.attributes, lambda key, value: lower(key))` |
| `map_filter(...)` | `map_filter` | `map_filter(order.attributes, lambda key, value: value.is_not_null())` |
| `map_zip_with(...)` | `map_zip_with` | `map_zip_with(o.left, o.right, lambda k, l, r: coalesce(l, r))` |

**Details And Differences**

- Transform and filter callbacks receive symbolic key/value expressions.
- Duplicate transformed keys are rejected by Structure's strict map contract.
- Python callback control flow and row-expanding generators such as `explode(...)` are unsupported. See
  [advanced analytical operations](../reference/AdvancedAnalyticalOperations.md).
