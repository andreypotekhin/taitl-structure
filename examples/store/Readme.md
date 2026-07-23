# Store Example App

This example models a multi-tenant retail order flow in an online e-commerce store. It turns incoming orders into validated, enriched shipment lines,
then demonstrates daily analytics with multidimensional summaries, collection operations, joins and projections. Structure owns the transformations; callers provide sources, persistence, stream lifecycle, and the
business actions taken from the results.

| Concern | Transform | Result | Details |
| --- | --- | --- | --- |
| Fulfillment | `EnrichOrders` | `OrderPublished` | Streaming-compatible order enrichment. |
| Daily analytics | `OrderAnalytics` | Customer and product daily results | Batch aggregation and windows. |
| Advanced analytics | `AdvancedOrderAnalytics` | Rollups, cubes, and profiles | Batch analytical examples. |
| Join shapes | `RowsetJoinExamples` | Reconciliations and candidates | Full, right, Cartesian joins. |
| Scalar features | `V3OrderFeatures` | `V3OrderProjection` | Batch expression reference. |

## Enrich orders

`EnrichOrders` accepts orders together with customer, product, blocked-product, promotion, and shipment reference
data. It is streaming-compatible: callers can supply an orders stream with static lookup relations, then choose the
checkpoint, trigger, sink, and start/stop lifecycle.

The transform rejects orders without an ID, customer ID, or product ID. It lowercases and trims identifiers and text
attributes, turns invalid or missing monetary values into zero, defaults a missing quantity to one, and removes rows
whose net total is negative. An order is marked `is_large` when its pre-discount total exceeds 1000.

```python
published = EnrichOrders(
    orders=orders,
    customers=customers,
    products=products,
    blocked_products=blocked_products,
    promotions=promotions,
    shipments=shipments,
).run(session).published

query = (
    published.writeStream.outputMode("append")
    .option("checkpointLocation", checkpoint)
    .format("memory")
    .start()
)
```

Customer enrichment is tenant-scoped and uses a broadcast left join, so a missing customer leaves the order visible
with null customer details. Product enrichment requires a tenant-scoped product and rejects blocked products. It uses a
latest-by-ingestion-time lookup, and ties are an error rather than an arbitrary product choice. Promotion lookup is
tenant-scoped and temporal: a promotion applies only when the order date lies in its validity range. Shipment matching
is an inner join, so publication represents fulfilled order lines only.

The published result includes customer, product, promotion, and shipment data plus `has_promotion`. The example also
shows streaming-safe raw hooks that retain the current order frame, remove invalid totals, observe lookup inputs, and
derive internal quality columns before projecting the public output schema.

## Build daily analytics

`OrderAnalytics` consumes fulfilled order lines. It publishes tenant-scoped customer-day totals, product-day summaries,
and customer event rankings.

```python
analytics = OrderAnalytics(fulfilled=fulfilled).run(session)
customer_totals = analytics.customer_totals
product_summary = analytics.product_summary
customer_rank = analytics.customer_event_rank

largest_customer_days = customer_totals.orderBy(customer_totals.net_total.desc())
```

Customer totals report order count plus gross and net revenue. Product summaries add distinct customer count, units,
minimum, maximum, average units, and gross revenue. The ranking path retains the latest order at each quantity per
customer, then emits row number, rank, dense rank, adjacent quantities, and three-row rolling unit measures.

## Explore advanced analytical expressions

`AdvancedOrderAnalytics` is a compact reference for higher-level Spark expressions. It publishes:

- `revenue_rollups`: tenant, category, and date rollups with subtotal flags, approximate statistics, correlations, and
  collected identifiers.
- `product_cubes`: tenant/category/customer-tier cubes with grouping IDs, counts, and revenue.
- `customer_windows`: customer-partitioned three-row windows with percentile, distribution, tile, positional, and
  running aggregate measures.
- `collection_profiles`: array and map normalization, set operations, safe element access, sorting, aggregation, and
  map transformations.

```python
advanced = AdvancedOrderAnalytics(fulfilled=fulfilled, collections=collections).run(session)
rollups = advanced.revenue_rollups
profiles = advanced.collection_profiles

category_rollups = rollups.orderBy("tenant_id", "grouping_id", "product_category")
```

Rollup and cube rows intentionally include grouped dimensions as null, so consumers use `grouping_id` and the provided
subtotal flag instead of interpreting null as a source value. Approximate metrics are descriptive results, not exact
accounting records.

## Join and feature examples

`RowsetJoinExamples` makes non-default relation shapes explicit: a full join reconciles orders and customers, a right
join keeps customers missing from reconciliation, and an intentionally allowed Cartesian join expands each retained
customer row against products. It is a join-semantics reference, not a recommended way to generate an unbounded catalog.

`V3OrderFeatures` projects a standalone order source into string, regular-expression, struct, cast, date, numeric,
and ranking features. Its strict integer cast illustrates an invalid-data failure path; `safe_quantity` uses a tolerant
cast for the same source field. Rows without `name` or `raw_quantity` are excluded before projection.

```python
features = V3OrderFeatures(orders=v3_orders).run(session).projected
valid_quantities = features.where("safe_quantity IS NOT NULL").orderBy("recency_rank")
```

## Result ownership

The example neither defines a customer-facing storefront nor chooses a warehouse layout. Callers own source quality,
reference-data freshness, checkpointing, durable output, monitoring, privacy controls, and the downstream decisions
made from order and customer data.
