# Joins API

Supported joins are compiler-visible, typed operations. Examples abbreviate the current `order` relation as `o`, an
unjoined `customer` relation as `c`, temporal price relation as `p`, and as-of rate relation as `r`; `j`, `t`, `f`,
and `e` denote a temporal predicate, event time, valid-from, and valid-to expression.

## Simple Joins

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `lookup_join(...)` | `DataFrame.join` | `lookup_join(on=o.customer_id == c.id)` |
| `exists(...)` | left-semi join | `where(exists(on=o.customer_id == c.id))` |
| `not_exists(...)` | left-anti join | `where(not_exists(on=o.customer_id == c.id))` |

**Details And Differences**

- `lookup_join(...)` enriches the current relation and defaults to `"left"`.
- `exists(...)` and `not_exists(...)` filter current rows without exposing right-side fields.
- `on=` is a symbolic predicate; same-name key shorthand is covered below.

## General Rowset Joins

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `rowset_join(...)` | `DataFrame.join` | `rowset_join(c, on=o.customer_id == c.id)` |
| `left_join(...)` | `join(..., how="left")` | `left_join(on=o.customer_id == c.id)` |
| `inner_join(...)` | `join(..., how="inner")` | `inner_join(on=o.customer_id == c.id)` |
| `right_join(...)` | `join(..., how="right")` | `right_join(on=o.customer_id == c.id)` |
| `full_join(...)` | `join(..., how="full")` | `full_join(on=o.customer_id == c.id)` |
| `cross_join(...)` | `crossJoin` | `cross_join(calendar, allow_cartesian=True)` |
| `relation_alias(...)` | DataFrame alias | `historical = relation_alias(customer, name="historical_customer")` |

**Details And Differences**

- `left_join(...)`, `inner_join(...)`, `right_join(...)`, `full_join(...)`, and `cross_join(...)` are shortcuts over
  `rowset_join(...)`.
- Right and full joins can produce null left-side fields. Build their output with an explicit constructor or projection.
- Cross joins require `allow_cartesian=True` and do not accept `on=`.
- Same-name key shorthand is supported: `left_join(on="customer_id")` and
  `inner_join(on=["tenant_id", "order_id"])`.
- `relation_alias(...)` creates a named typed occurrence of the current rowset or an unjoined relation for a self join.
  The name must be a unique non-empty Python identifier within the step; aliasing does not execute or duplicate data.

## Time-Aware Joins

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `temporal_one(...)` | Temporal join | `temporal_one(on=o.id == p.id, at=o.at, valid_from=p.from_, valid_to=p.to)` |
| `as_of_one(...)` | As-of match | `as_of_one(on=o.id == r.id, left_time=o.at, right_time=r.at)` |
| `direction=` | Direction | `as_of_one(on=o.id == r.id, left_time=o.at, right_time=r.at, direction="nearest")` |
| `overlaps=` | Overlap policy | `temporal_one(on=j, at=t, valid_from=f, valid_to=e, overlaps="error")` |
| `ties=` | Selection policy | `as_of_one(on=o.id == r.id, left_time=o.at, right_time=r.at, ties="error")` |

**Details And Differences**

- `temporal_one(...)` selects one right row valid at `at`; its normal interval is closed-open.
- `as_of_one(...)` supports backward, forward, and nearest directions. Nearest matching ignores null time candidates,
  ranks by absolute distance, and fails equidistant best matches when `ties="error"`.
- Explicit overlap and tie policies keep selection rules reviewable. Directional nearest tie preferences remain outside
  the public API; make the right-side time unique or narrow candidates with `tolerance=`.

## Hints And Corner Cases

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `how=` | PySpark `how` values | `rowset_join(c, on=o.customer_id == c.id, how="left")` |
| `hint=` | DataFrame hint | `lookup_join(on=o.customer_id == c.id, hint="broadcast")` |
| `strategy=` | Join hint | `inner_join(on=o.id == c.id, strategy="shuffle_hash")` |
| `JoinDedupe.latest_by(...)` | `row_number` pre-dedupe | `JoinDedupe.latest_by(c.at)` |
| `JoinDedupe.earliest_by(...)` | `row_number` pre-dedupe | `JoinDedupe.earliest_by(c.at)` |

**Details And Differences**

- String options are validated and normalized before compilation; enum constants remain accepted as aliases.
- Dedupe is only for lookup joins and must make the right-row selection rule explicit.
- Raw SQL join predicates are unsupported. See the [Transforms background](../background/Transform.back.md).
