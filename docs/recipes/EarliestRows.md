# Earliest Rows

**Problem:** A purchase history can contain several orders for one customer, while analytics needs the first purchase
for each customer and region with its original identifier and amount.

| region | customer_id | order_id | sequence | amount |
| --- | --- | --- | ---: | ---: |
| `west` | `C-100` | `o-10` | 10 | 45.00 |
| `west` | `C-100` | `o-11` | 11 | 20.00 |
| `east` | `C-100` | `o-12` | 3 | 32.00 |
| `east` | `C-200` | `o-20` | 7 | 18.00 |

**Solution:** Use `dedupe_earliest_by(...)` with customer and region as the composite partition and sequence as the
ordering value. The example keeps `o-10` for west `C-100`, `o-12` for east `C-100`, and `o-20` for east `C-200`.
`earliest_by(...)` has the same selection behavior and can be clearer when the result is described as choosing a first
row.

## Define the row contracts

Declare the fields needed to identify a purchase and to carry the winning row into the output.

```python
from structure import *
from structure.plugin.pyspark import *


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

Both contracts include the business key, ordering value, and purchase details. The output can instead expose only the
fields downstream consumers need.

## Keep the first purchase

Select the first row before projecting it. Passing a list to `partition_by` makes the composite business key visible at
the point where the rule is defined.

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
the fields from that winning row; it is not an aggregation, so it does not reconstruct `order_id` or `amount` from
separate aggregate values.

## Run the transform

Run the transform with a DataFrame matching `Purchase`, then retrieve the named output.

```python
from structure import *


session = StructureSession(spark=spark)
result = FirstPurchases(purchases=purchases_df).run(session)
first_purchases_df = result.first
```

`first_purchases_df` contains the three earliest purchases from the example feed.

## Choose ordering and partition keys

Use an ordering value that represents business time unambiguously. A feed offset or ingestion timestamp may be wrong
when late events arrive; an immutable purchase sequence or the event's authoritative occurrence time is usually a
better choice. Keep the partition key aligned with the question the output answers:

- First purchase per customer: `partition_by=purchase.customer_id`.
- First purchase per customer and region: `partition_by=[purchase.region, purchase.customer_id]`.
- First purchase per customer, region, and campaign: add `purchase.campaign_id` to that list.

Ties are not a silent "pick either" case. The current public policy is `"error"`, so ensure the chosen ordering gives
each partition one earliest row before relying on the result. These helpers are batch-only; use a batch input here.

For the complete helper contract, see [Latest and Earliest Rows](../QuickRef.md#latest-and-earliest-rows) and the
[Transform background](../background/Transform.back.md).
