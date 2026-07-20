# Earliest Rows

Use this recipe to retain the first observed row for each business key from a batch history. Typical uses include a
customer's first purchase, the first status recorded for an order, or the first version of a configuration.

This recipe uses `dedupe_earliest_by(...)` because the result is a keyed-deduplicated history. `earliest_by(...)` has
the same selection behavior and can be clearer when the result is described as choosing a first row.

## Scenario

A purchase feed can contain several orders for one customer. Analytics needs the first purchase per customer and
region, preserving the original order identifier and amount.

| region | customer_id | order_id | sequence | amount |
| --- | --- | --- | ---: | ---: |
| `west` | `C-100` | `o-10` | 10 | 45.00 |
| `west` | `C-100` | `o-11` | 11 | 20.00 |
| `east` | `C-100` | `o-12` | 3 | 32.00 |
| `east` | `C-200` | `o-20` | 7 | 18.00 |

The output keeps `o-10` for the west `C-100` customer, `o-12` for the east `C-100` customer, and `o-20` for the east
`C-200` customer. The region is part of the identity: the same customer can have an independent first purchase in each
region.

## Define The Row Contracts

```python
from structure import Schema, Transform, input, output, transform
from structure.platform.pyspark import *


class Purchase(Schema):
    region = string(nullable=False)
    customer_id = string(nullable=False)
    order_id = string(nullable=False)
    sequence = long(nullable=False)
    amount = decimal(12, 2, nullable=False)


class FirstPurchase(Schema):
    region = string(nullable=False)
    customer_id = string(nullable=False)
    order_id = string(nullable=False)
    sequence = long(nullable=False)
    amount = decimal(12, 2, nullable=False)
```

## Keep The First Purchase

The selection belongs before the output projection. Passing a list to `partition_by` makes the composite business key
visible at the point where the rule is defined.

```python
@transform
class FirstPurchases(Transform):
    purchases = input(Purchase)
    first = output(FirstPurchase)

    def select_first(self, purchase: Purchase) -> FirstPurchase:
        dedupe_earliest_by(
            purchase.sequence,
            partition_by=[purchase.region, purchase.customer_id],
        )
        return FirstPurchase(
            region=purchase.region,
            customer_id=purchase.customer_id,
            order_id=purchase.order_id,
            sequence=purchase.sequence,
            amount=purchase.amount,
        )
```

`dedupe_earliest_by(...)` retains the row with the smallest `sequence` in each `(region, customer_id)` group. It keeps
all the fields from that winning row; it is not an aggregation, so it does not need to reconstruct `order_id` or
`amount` from separate aggregate values.

## Run It

```python
from structure import (
    Schema,
    StructureConfig,
    StructureSession,
    StructureTools,
    Transform,
    input,
    lane,
    output,
    raw,
    special,
    step,
    transform,
)


session = StructureSession(spark=spark)
result = FirstPurchases(purchases=purchases_df).run(session)
first_purchases_df = result.first
```

## Choose The Right Ordering And Boundary

Use an ordering value that represents business time unambiguously. A feed offset or ingestion timestamp may be wrong
when late events arrive; an immutable purchase sequence or the event's authoritative occurrence time is usually a
better choice. Keep the partition key aligned with the question the output answers:

- First purchase per customer: `partition_by=purchase.customer_id`.
- First purchase per customer and region: `partition_by=[purchase.region, purchase.customer_id]`.
- First purchase per customer, region, and campaign: add `purchase.campaign_id` to that list.

As with latest-row selection, ties are not a silent "pick either" case. The current public policy is
`TiePolicy.ERROR`, so ensure the chosen ordering gives each partition one earliest row before relying on the result.
These helpers are batch-only; do not use this recipe for a streaming input.

For the complete helper contract, see [Latest and Earliest Rows](../QuickRef.md#latest-and-earliest-rows) and the
[DSL reference](../background/DSL.back.md#selected-row-dedupe).
