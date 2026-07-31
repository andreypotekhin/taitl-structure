# Store Example App

This example models a multi-tenant retail, merchandising, and fulfillment flow in an online e-commerce store. It can
shape demand before an order exists by ranking product recommendations, then turn incoming orders into validated
commercial demand, plan warehouse allocation and backorders from inventory facts, keep actual shipment publication
separate, and demonstrate operational summaries. Structure owns the transformations; callers provide sources,
persistence, stream lifecycle, and the business actions taken from the results.

| Concern | Transform | Result | Details |
| --- | --- | --- | --- |
| Catalog preparation | `PrepareCatalog` | `CatalogProduct` | Tenant-visible product facts for recommendation eligibility. |
| Recommendations | `Recommender` / `Merchandising` | `RecommendedProduct`, `RecommendationRun` | Transparent policy, promotion, and feedback-aware product ranking. |
| Merchandising feedback | `BuildRecommendationSignals` | Daily facts and product signals | Impression/click attribution and reusable product signals. |
| Merchandising evaluation | `EvaluateRecommendations` | Request and daily behavior summaries | Zero-result, click, and exposure-aware behavior evaluation. |
| Catalog normalization and taxonomy | `NormalizeCatalog` / `ExpandProductTaxonomy` | Normalized catalog and ancestor facts | Stable tenant-scoped category joins with bounded hierarchy expansion. |
| Session and purchase feedback | `BuildSessionSignals` / `BuildRecommendationPurchaseSignals` | Session features and attributed purchases | Event-time bounded interests and explicit recommendation-attribution status. |
| Candidate stages | `GenerateRecommendationCandidates` / `FilterRecommendationCandidates` | Candidates and decision evidence | Retrieve from catalog, taxonomy, session, and feedback sources, then filter with reasons. |
| Diversification | `DiversifyRecommendations` | Diverse ranked products | Deterministic taxonomy-branch caps after ranking. |
| Recommendation experiments | `AssignRecommendationVariants` / `EvaluateRecommendationExperiment` | Assignments, exposures, variant metrics | Stable tenant-scoped assignments and descriptive observed-variant comparisons. |
| Fulfillment pipeline | `Fulfillment` | Demand, plans, suggestions, summaries | Main planning boundary from commercial order inputs to fulfillment outputs. |
| Demand preparation | `PrepareOrderDemand` | `Order` | Valid commercial order lines before warehouse or shipment decisions. |
| Fulfillment planning | `PlanFulfillment` | Allocations, backorders, plans, suggestions | Deterministic warehouse selection and conservative replenishment signals. |
| Planning analytics | `FulfillmentAnalytics` | Daily and warehouse load summaries | Batch service-risk and load summaries. |
| Plan versus actual | `ReconcileFulfillmentPlan` | `FulfillmentReconciliation` | Planned lines observed later in shipment facts. |
| Dated inventory | `BuildDemandWindows` / `ProjectInventory` | `DemandWindow`, `InventoryProjection` | Observed demand windows, inbound receipts, lead-time-aware usable dates, and projected stock. |
| Shortages and substitutions | `DetectShortages` / `FindSubstitutions` | `FulfillmentShortage`, `FulfillmentSubstitutionOption` | First projected deficit and ranked policy-approved alternatives. |
| Operational exceptions | `PrioritizeExceptions` | `FulfillmentException` | Stable priority queue exposing shortage, late-inbound, service-risk, and substitution reasons. |
| Service evaluation | `EvaluateFulfillment` | `FulfillmentServiceEvaluation` | Line-safe on-time, in-full, lateness, and target-attainment results from actual shipments. |
| Enrichment | `EnrichOrders` | `OrderPublished` | Streaming-compatible order enrichment. |
| Daily analytics | `OrderAnalytics` | Customer and product daily results | Batch aggregation and windows. |
| Advanced analytics | `AdvancedOrderAnalytics` | Rollups, cubes, and profiles | Batch analytical examples. |
| Join shapes | `RowsetJoinExamples` | Reconciliations and candidates | Full, right, Cartesian joins. |

## Recommendations and merchandising

The merchandising path is separate from order fulfillment. It answers “which products should we show before demand
exists?” while fulfillment planning answers “how should we serve demand that already exists?”

`PrepareCatalog` consumes products, blocked products, and promotions. It keeps only active, tenant-scoped products that
are not blocked. Promotions are descriptive ranking evidence: a matching product or category promotion raises the
promotion score, but it does not force an ineligible product into the result set.

`Recommender` consumes recommendation requests, a prepared catalog, tenant-owned policy, boost and suppression rules,
and optional historical product signals. The first scoring model is deliberately transparent: base eligibility,
promotion score, policy boost, suppression penalty, inventory boost, and feedback score each remain separate fields.
Ranking uses the exact tie-breakers: higher final score, lower suppression penalty, higher inventory boost, then
product ID.

```python
recommendations = Recommender(
    requests=requests,
    catalog=catalog,
    policy=policy,
    boosts=boosts,
    suppressions=suppressions,
    signals=signals,
).run(session)

ranked = recommendations.recommended_products
runs = recommendations.recommendation_runs
```

`Merchandising` lives in the main merchandising package and captures the full serving pipeline from products to ranked
recommendation rows. It builds feedback signals from streaming impressions, clicks, and fulfilled orders, then uses
those signals for recommendation ranking. Evaluation is a separate workflow under `transforms/evaluation/recommender/behavior/`, so serving
does not require evaluation inputs.

```python
merch = Merchandising(
    requests=requests,
    products=products,
    blocked_products=blocked_products,
    promotions=promotions,
    policy=policy,
    boosts=boosts,
    suppressions=suppressions,
    feedback_impressions=feedback_impressions,
    feedback_clicks=feedback_clicks,
).run(session)

evaluation = EvaluateRecommendations(
    batch=evaluation_batch,
    requests=evaluation_requests,
    impressions=evaluation_impressions,
    clicks=evaluation_clicks,
).run(session)
```

`BuildRecommendationSignals` and `BuildRecommendationPurchaseSignals` live under `merchandising/signals/`. They turn
recommendation impressions, timely clicks, and fulfilled orders into daily facts and product-level signals. A click
counts only when it references an impression and happens within 24 hours of that impression; purchase attribution uses
the fulfilled-order stream and a 30-day impression boundary.
`EvaluateRecommendations` keeps zero-result requests in the denominator and summarizes daily behavior by strategy and
policy version, including zero-result rate, clicked-request rate, mean first-click rank, and exposure-adjusted click
rate.

The complete recommendation path makes the decision stages visible: catalog identifiers and categories are normalized,
products expand through a bounded taxonomy, session events become one-day features, and fulfilled order facts cross a
documented recommendation-exposure boundary before becoming purchase feedback. Candidates record whether they came from
category retrieval, session interest, or popularity feedback. Filtering records hard-suppression and session-exclusion
reasons, ranking preserves score components, and diversification applies a deterministic taxonomy-branch cap last.

Recommendation experiments use a stable hash of the tenant-scoped customer, session, or request key. An exposure is
recorded only for a served recommendation run, and variant evaluation reports observed request, impression, click,
purchase, and declared guardrail metrics. These summaries describe observed behavior; they do not claim causal impact.

## Fulfillment planning

The planning path is separate from shipment-backed publication. `PrepareOrderDemand` uses the same commercial checks as
`EnrichOrders` through customer, product, blocked-product, and promotion enrichment, but it stops before shipment
matching. Its `Order` output means “this order line is commercially valid and ready to fulfill.”

`Fulfillment` lives in the fulfillment transform package and captures the overall planning pipeline. It composes
`PrepareOrderDemand`, `PlanFulfillment`, `ReconcileFulfillmentPlan`, and `FulfillmentAnalytics`, exposing demand,
allocation, backorder, plan, reconciliation, replenishment, daily-summary, and warehouse-load outputs from one
app-facing transform.

```python
fulfillment = Fulfillment(
    orders=orders,
    customers=customers,
    products=products,
    blocked_products=blocked_products,
    promotions=promotions,
    warehouses=warehouses,
    inventory_positions=inventory_positions,
    inbound_inventory=inbound_inventory,
    lead_times=lead_times,
    substitution_rules=substitution_rules,
    service_targets=service_targets,
    fulfilled=fulfilled,
).run(session)
```

`PlanFulfillment` consumes `Order`, active warehouses, inventory positions, and inbound inventory. It computes
available-to-promise as on-hand quantity minus reserved quantity, never below zero. Inbound inventory does not increase
immediate allocation; it only supplies a possible planned ship date for partial or full backorders and informs
replenishment suggestions.

For each demand line, warehouse options are tenant-scoped and active. The planner selects one preferred option by the exact
tie-breakers: same customer region first, lower warehouse priority value, greater available-to-promise quantity, then
warehouse ID. The first version does not split one order line across multiple warehouses. It allocates from the best
warehouse when possible, partially allocates when the best warehouse has some stock, and backorders the full line when
the best warehouse has none.

```python
demand = PrepareOrderDemand(
    orders=orders,
    customers=customers,
    products=products,
    blocked_products=blocked_products,
    promotions=promotions,
).run(session).demand

plan = PlanFulfillment(
    demand=demand,
    warehouses=warehouses,
    inventory_positions=inventory_positions,
    inbound_inventory=inbound_inventory,
).run(session)

allocations = plan.allocations
backorders = plan.backorders
plans = plan.plans
suggestions = plan.replenishment_suggestions
```

`FulfillmentPlan.plan_status` is one of `allocated`, `partially_allocated`, or `backordered`. A planned ship date equal
to the order date means the line can be allocated immediately. A later planned ship date comes from inbound inventory
and is a planning signal, not a guaranteed shipment promise. A null planned ship date means no inbound fact is known.

`ReplenishmentSuggestion` is descriptive. It appears only when available-to-promise after the plan falls below safety
stock and inbound inventory is late for the order date or absent. It does not imply an automated purchase order,
transfer, or reservation.

`FulfillmentAnalytics` in `transforms/analytics/fulfillment/` summarizes the plan by tenant and business date, and summarizes allocation load by warehouse.
`ReconcileFulfillmentPlan` compares `FulfillmentPlan` with actual `OrderFulfillment` shipment facts. Shipments do not
currently carry warehouse identity, so reconciliation reports whether a planned line later shipped, not whether it
shipped from the planned warehouse.

The remaining fulfillment outputs make projections and observations explicit. `BuildDemandWindows` aggregates observed
`Order` demand by product and date; it does not forecast. `ProjectInventory` applies on-hand, reservations, inbound
receipts, and declared `LeadTime` facts across those windows. `DetectShortages` publishes the first window
that falls below safety stock. `FindSubstitutions` ranks only tenant-scoped, active `SubstitutionRule`
alternatives and never rewrites the original order line.

`PrioritizeExceptions` exposes the inputs to its priority policy—shortage size, lateness, customer tier, and
service target—instead of hiding them in an opaque score. `EvaluateFulfillment` matches shipments by tenant,
order ID, and `line_number`, so duplicate products on one order remain distinct. A missing shipment date stays
unknown, while observed dates are classified as on time or late and full or partial. Warehouse fields in service
summaries refer to the planned warehouse because `Shipment` does not yet carry actual warehouse identity.

The composed `Fulfillment` result also exposes `demand_windows`, `inventory_projections`, `shortages`,
`substitution_options`, `exceptions`, `service_evaluations`, and `daily_service_summary` alongside the original plan,
allocation, backorder, reconciliation, and replenishment outputs.

## Enrichment

`EnrichOrders` accepts orders together with customer, product, blocked-product, promotion, and shipment reference
data. It is streaming-compatible: callers can supply an orders stream with static lookup relations, then choose the
checkpoint, trigger, sink, and start/stop lifecycle.

The enrichment transform rejects orders without an ID, customer ID, or product ID. It lowercases and trims identifiers and text
attributes, turns invalid or missing monetary values into zero, defaults a missing quantity to one, and removes rows
whose net total is negative. An order is marked `is_large` when its pre-discount total exceeds 1000.

Customer enrichment is tenant-scoped and uses a broadcast left join, so a missing customer leaves the order visible
with null customer details. Product enrichment requires a tenant-scoped product and rejects blocked products. It uses a
latest-by-ingestion-time lookup, and ties are an error rather than an arbitrary product choice. Promotion lookup is
tenant-scoped and temporal: a promotion applies only when the order date lies in its validity range. Shipment matching
is an inner join, so publication represents actual fulfilled order lines only. Planned allocations and actual shipment
facts deliberately remain different relations with different meanings.

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

The published result includes customer, product, promotion, and shipment data plus `has_promotion`. Invalid net totals
are removed by an ordinary streaming-compatible transformation step before lookup and publication.

## Daily analytics

`OrderAnalytics` consumes fulfilled order lines. It publishes tenant-scoped customer-day totals, product-day summaries,
and customer event rankings.

Customer totals report order count plus gross and net revenue. Product summaries add distinct customer count, units,
minimum, maximum, average units, and gross revenue. The ranking path retains the latest order at each quantity per
customer, then emits row number, rank, dense rank, adjacent quantities, and three-row rolling unit measures.

```python
analytics = OrderAnalytics(fulfilled=fulfilled).run(session)
customer_totals = analytics.customer_totals
product_summary = analytics.product_summary
customer_rank = analytics.customer_event_rank

largest_customer_days = customer_totals.orderBy(customer_totals.net_total.desc())
```

## Advanced analytical expressions

`AdvancedOrderAnalytics` is a demonstration of higher-level Spark expressions. It publishes:

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

Rollup and cube rows include grouped dimensions as null, so consumers use `grouping_id` and the provided
subtotal flag instead of interpreting null as a source value. Approximate metrics are descriptive results, not exact
accounting records.

## Join examples

`RowsetJoinExamples` demonstrates rowset join shapes: a full join reconciles orders and customers, a right
join keeps customers missing from reconciliation, and an intentionally allowed Cartesian join expands each retained
customer row against products. It is a join-semantics reference, not a recommended way to generate an unbounded catalog.

## Result ownership

The example neither defines a customer-facing storefront nor owns warehouse topology. Callers own source quality,
reference-data freshness, inventory accuracy, checkpointing, durable output, monitoring, privacy controls, and the
downstream decisions made from recommendations, demand, planning, shipment, order, and customer data.
