# Latest Rows

Use this recipe to keep the current event for each account from a batch event feed. It is useful for a latest-state
table, a current customer profile, or any pipeline where each business key must retain exactly one most-recent row.

This recipe uses `latest_by(...)`. `dedupe_latest_by(...)` has the same behavior; prefer that name when the business
intent is specifically keyed deduplication.

## Scenario

An upstream system sends account events with a monotonic sequence number. A downstream table needs only the current
event for each account.

| account_id | event_id | sequence | status |
| --- | --- | ---: | --- |
| `A-100` | `e-10` | 10 | `pending` |
| `A-100` | `e-11` | 11 | `active` |
| `B-200` | `e-20` | 4 | `pending` |
| `B-200` | `e-21` | 5 | `suspended` |

The required output is one row for `A-100` at sequence `11` and one for `B-200` at sequence `5`.

## Define The Row Contracts

Keep the input contract broad enough to represent the feed and make the output contract say exactly what downstream
consumers receive.

```python
from structure import *
from structure.platform.pyspark.dsl.field import long, string


class AccountEvent(Schema):
    account_id = string(nullable=False)
    event_id = string(nullable=False)
    sequence = long(nullable=False)
    status = string(nullable=False)


class CurrentAccountEvent(Schema):
    account_id = string(nullable=False)
    event_id = string(nullable=False)
    sequence = long(nullable=False)
    status = string(nullable=False)
```

## Select The Current Row

Declare the input and output, then select the latest row before projecting it. `partition_by` says which rows compete
with one another. `order_by` says which competing row is current.

```python
@transform
class CurrentAccountEvents(Transform):
    events = input(AccountEvent)
    current = output(CurrentAccountEvent)

    def select_current(self, event: AccountEvent) -> CurrentAccountEvent:
        latest_by(event.sequence, partition_by=event.account_id)
        return CurrentAccountEvent(
            account_id=event.account_id,
            event_id=event.event_id,
            sequence=event.sequence,
            status=event.status,
        )
```

Structure retains the row with the greatest `sequence` in every `account_id` partition. The selection is part of the
compiled transform, so it works the same way in execution and generated-code execution; it does not need a raw hook or
an unreviewable `dropDuplicates(...)` call.

## Run It

Pass a DataFrame matching `AccountEvent` to the transform and retrieve the named output.

```python
from structure import *


session = StructureSession(spark=spark)
result = CurrentAccountEvents(events=events_df).run(session)
current_events_df = result.current
```

`current_events_df` contains the rows for `e-11` and `e-21` in the scenario above.

## Make The Ordering A Business Rule

The ordering value must distinguish the winning row within each partition. The current public tie policy is
`TiePolicy.ERROR`: equal ordering values do not express a valid choice of winner. Treat that as an upstream data
quality issue and provide a sequence, version, or other unique business ordering before this transform runs.

Use the smallest business key that identifies one current entity. For example, if account identifiers repeat across
tenants, partition by both values:

```python
latest_by(
    event.sequence,
    partition_by=[event.tenant_id, event.account_id],
)
```

Selected-row helpers are batch-only. A streaming current-state table needs explicit state and watermark semantics, so
use a batch input for this recipe.

For the complete helper contract, see [Latest and Earliest Rows](../QuickRef.md#latest-and-earliest-rows) and the
[DSL reference](../background/DSL.back.md#selected-row-dedupe).
