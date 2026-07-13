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

- `lookup_join(...)` enriches the current relation and defaults to `Join.LEFT`.
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

**Details And Differences**

- `left_join(...)`, `inner_join(...)`, `right_join(...)`, `full_join(...)`, and `cross_join(...)` are shortcuts over
  `rowset_join(...)`.
- Right and full joins can produce null left-side fields. Build their output with an explicit constructor or projection.
- Cross joins require `allow_cartesian=True` and do not accept `on=`.
- Same-name key shorthand is supported: `left_join(on="customer_id")` and
  `inner_join(on=["tenant_id", "order_id"])`.

## Time-Aware Joins

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `temporal_one(...)` | Temporal join | `temporal_one(on=o.id == p.id, at=o.at, valid_from=p.from_, valid_to=p.to)` |
| `as_of_one(...)` | As-of match | `as_of_one(on=o.id == r.id, left_time=o.at, right_time=r.at)` |
| `AsOf` | — | `direction = AsOf.FORWARD` |
| `OverlapPolicy` | Overlap | `temporal_one(on=j, at=t, valid_from=f, valid_to=e, overlaps=OverlapPolicy.ERROR)` |
| `TiePolicy` | Selection policy | `as_of_one(on=o.id == r.id, left_time=o.at, right_time=r.at, ties=TiePolicy.ERROR)` |

**Details And Differences**

- `temporal_one(...)` selects one right row valid at `at`; its normal interval is closed-open.
- `as_of_one(...)` supports backward and forward directions. Nearest as-of matching remains future work.
- Explicit overlap and tie policies keep selection rules reviewable.

## Hints And Corner Cases

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `Join` | PySpark `how` values | `rowset_join(c, on=o.customer_id == c.id, how=Join.LEFT)` |
| `JoinHint` | DataFrame hint | `lookup_join(on=o.customer_id == c.id, hint=JoinHint.BROADCAST)` |
| `JoinStrategy` | Join hint | `inner_join(on=o.id == c.id, strategy=JoinStrategy.SHUFFLE_HASH)` |
| `JoinDedupe.latest_by(...)` | `row_number` pre-dedupe | `JoinDedupe.latest_by(c.at)` |
| `JoinDedupe.earliest_by(...)` | `row_number` pre-dedupe | `JoinDedupe.earliest_by(c.at)` |

**Details And Differences**

- `Join`, `JoinHint`, and `JoinStrategy` replace unvalidated string options with capability-checked values.
- Dedupe is only for lookup joins and must make the right-row selection rule explicit.
- Raw SQL join predicates are unsupported. See the [Transforms reference](../background/DSL.back.md).
