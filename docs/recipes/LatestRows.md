# Latest Rows

**Problem:** An account event feed can contain several rows per account, while a downstream current-state table needs
exactly one most-recent row for each account.

| account_id | event_id | sequence | status |
| --- | --- | ---: | --- |
| `A-100` | `e-10` | 10 | `pending` |
| `A-100` | `e-11` | 11 | `active` |
| `B-200` | `e-20` | 4 | `pending` |
| `B-200` | `e-21` | 5 | `suspended` |

**Solution:** Use `latest_by(...)` with the account key as the partition and the event sequence as the ordering value.
The example therefore keeps `e-11` for `A-100` and `e-21` for `B-200`. `dedupe_latest_by(...)` has the same behavior;
prefer that name when the business intent is specifically keyed deduplication.

## Define the row contracts

Start with an input contract broad enough to represent the feed and an output contract that says exactly what
downstream consumers receive.

```python
from structure import *
from structure.plugin.pyspark import *


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

`AccountEvent` supplies the competing rows. `CurrentAccountEvent` keeps the same fields here, but an output contract
can also narrow or rename the selected row for its consumers.

## Select the current row

Declare the input and output, then select the latest row before projecting it. `partition_by` identifies the rows that
compete with one another; the first argument to `latest_by(...)` identifies which competing row is current.

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

Structure retains the row with the greatest `sequence` in every `account_id` partition. Because the selection is part
of the compiled transform, execution and generated-code execution apply the same rule without a raw hook or a
`dropDuplicates(...)` call.

## Run the transform

Run the transform with a DataFrame matching `AccountEvent`, then retrieve the named output.

```python
from structure import *


session = StructureSession(spark=spark)
result = CurrentAccountEvents(events=events_df).run(session)
current_events_df = result.current
```

`current_events_df` contains the rows for `e-11` and `e-21` from the example feed.

## Choose ordering and partition keys

Choose an ordering value that distinguishes the winning row within each partition. The current public tie policy is
`"error"`: equal ordering values do not express a valid choice of winner. Treat that as an upstream data-quality
issue and provide a sequence, version, or other unique business ordering before this transform runs.

Use the smallest business key that identifies one current entity. For example, if account identifiers repeat across
tenants, partition by both values:

```python
latest_by(
    event.sequence,
    partition_by=[event.tenant_id, event.account_id],
)
```

Selected-row helpers are batch-only. A streaming current-state table needs explicit state and watermark semantics, so
use a batch input here.

For the complete helper contract, see [Latest and Earliest Rows](../QuickRef.md#latest-and-earliest-rows) and the
[Transform background](../background/Transform.back.md).
