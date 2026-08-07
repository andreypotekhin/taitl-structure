# Store Example App

Focused boundary contracts are collected in the [Store example specifications](../../docs/examples/store/Readme.md).

This example models a multi-tenant retail, merchandising, and fulfillment flow in an online e-commerce store. It can
shape demand before an order exists by ranking product recommendations, then turn incoming orders into validated
commercial demand, plan warehouse allocation and backorders from inventory facts, keep actual shipment publication
separate, and demonstrate operational summaries. Structure owns the transformations; callers provide sources,
persistence, stream lifecycle, and the business actions taken from the results.

### Source fixtures

Small, typed-source-oriented CSV fixtures live under [`examples/fixtures/store/`](../fixtures/store/). They cover the main order, fulfillment,
catalog, taxonomy, and recommendation paths, including multiple tenants, repeated order products on distinct lines,
inventory outcomes, and nullable lookup facts. The fixtures are intentionally representative rather than exhaustive;
tests compare selected rows and columns instead of asserting the entire dataset.

| Funnel stage | Transform package | Transform | Result | Details |
| --- | --- | --- | --- | --- |
| **1. Product foundation** | `transforms/catalog/`, `transforms/taxonomy/` | — | — | Establish tenant-scoped product facts before serving or ordering. |
| | `transforms/catalog/` | `PrepareCatalog` | `CatalogProduct` | Tenant-visible product facts for recommendation eligibility. |
| | `transforms/catalog/` | `NormalizeCatalog` | Normalized catalog | Stable identifiers and category joins for downstream transforms. |
| | `transforms/taxonomy/` | `ExpandProductTaxonomy` | Ancestor facts | Bounded hierarchy expansion for category-aware retrieval and ranking. |
| **2. Candidate and recommendation serving** | `transforms/recommender/`, `transforms/personalization/`, `transforms/merchandising/` | — | — | Turn product facts and shopper context into ranked recommendations. |
| | `transforms/recommender/`, `transforms/merchandising/` | `BuildRecommendationCandidates` / `Recommender` / `Merchandising` | Candidates, `RecommendedProduct`, `RecommendationRun` | Admit candidates, enrich them with context, and rank with transparent policy and behavior signals. |
| | `transforms/recommender/` | `DiversifyRecommendations` | Diverse ranked products | Deterministic taxonomy-branch caps after ranking. |
| | `transforms/recommender/signals/`, `transforms/merchandising/` | `BuildProductSignals` / `BuildSessionSignals` / `BuildPurchaseSignals` | Daily facts and product signals | Derive reusable signals from impressions, clicks, sessions, and attributed purchases. |
| | `transforms/evaluation/recommender/` | `EvaluateRecommendations` | Request and daily behavior summaries | Zero-result, click, and exposure-aware behavior evaluation. |
| | `transforms/experiments/` | `AssignRecommendationVariants` / `EvaluateRecommendationExperiment` | Assignments, exposures, variant metrics | Stable tenant-scoped assignments and descriptive observed-variant comparisons. |
| **3. Commercial demand admission** | `transforms/fulfillment/demand/` | — | — | Convert incoming orders into valid demand before warehouse or shipment decisions. |
| | `transforms/fulfillment/demand/` | `PrepareOrderDemand` | `Order` | Valid commercial order lines ready for fulfillment planning. |
| **4. Fulfillment planning** | `transforms/fulfillment/` and its `inventory/`, `planning/`, `shortages/`, and `substitutions/` subpackages | — | — | Decide how demand can be served from inventory and expose planning risk. |
| | `transforms/fulfillment/` | `Fulfillment` | Demand, plans, suggestions, summaries | Main planning boundary from commercial order inputs to fulfillment outputs. |
| | `transforms/fulfillment/planning/` | `PlanFulfillment` | Allocations, backorders, plans, suggestions | Deterministic warehouse selection and conservative replenishment signals. |
| | `transforms/fulfillment/demand/`, `transforms/fulfillment/inventory/` | `BuildDemandWindows` / `ProjectInventory` | `DemandWindow`, `InventoryProjection` | Observed demand windows, inbound receipts, lead-time-aware usable dates, and projected stock. |
| | `transforms/fulfillment/shortages/`, `transforms/fulfillment/substitutions/` | `DetectShortages` / `FindSubstitutions` | `FulfillmentShortage`, `FulfillmentSubstitutionOption` | First projected deficit and ranked policy-approved alternatives. |
| | `transforms/fulfillment/shortages/` | `PrioritizeExceptions` | `FulfillmentException` | Stable priority queue exposing shortage, late-inbound, service-risk, and substitution reasons. |
| **5. Shipment-backed publication and actuals** | `transforms/orders/`, `transforms/fulfillment/reconciliation/`, `transforms/evaluation/fulfillment/` | — | — | Keep observed shipment facts distinct from plans and commercial demand. |
| | `transforms/orders/` | `EnrichOrders` | `OrderPublished` | Streaming-compatible order enrichment joined to observed shipment facts. |
| | `transforms/fulfillment/reconciliation/` | `ReconcileFulfillmentPlan` | `FulfillmentReconciliation` | Planned lines observed later in shipment facts. |
| | `transforms/evaluation/fulfillment/` | `EvaluateFulfillment` | `FulfillmentServiceEvaluation` | Line-safe on-time, in-full, lateness, and target-attainment results from actual shipments. |
| **6. Analytics and teaching shapes** | `transforms/analytics/`, `transforms/rowset_joins/` | — | — | Summarize outcomes and demonstrate reusable join shapes after the main paths. |
| | `transforms/analytics/fulfillment/` | `FulfillmentAnalytics` | Daily and warehouse load summaries | Batch service-risk and load summaries. |
| | `transforms/analytics/orders/` | `OrderAnalytics` | Customer and product daily results | Batch aggregation and windows. |
| | `transforms/adv_analytics.py` | `AdvancedOrderAnalytics` | Rollups, cubes, and profiles | Batch analytical examples. |
| | `transforms/rowset_joins/` | `RowsetJoinExamples` | Reconciliations and candidates | Full, right, and Cartesian joins. |

Store keeps reusable domain boundaries at the top level: catalog preparation lives under `transforms/catalog/`,
taxonomy expansion under `transforms/taxonomy/`, recommendation serving under `transforms/recommender/`, fulfillment
planning under `transforms/fulfillment/`, and order enrichment under `transforms/orders/`. The `Merchandising` facade
composes its recommendation boundaries, while reporting remains under the analytics packages. The table follows the
data funnel from product facts to recommendations, demand, planning, shipment-backed actuals, and analytics; the
section rows identify the package boundary responsible for each stage.

### Inventory, warehousing, and shipping package boundaries

Inventory is already a first-class concern inside fulfillment: `transforms/fulfillment/inventory/` owns dated inventory
projection, while planning consumes inventory positions and inbound facts. The next architectural question is whether
inventory should become a sibling top-level package. That promotion is justified when inventory has reusable workflows
independent of an order plan—such as snapshot normalization, reservation reconciliation, availability publication, or
inventory quality checks consumed by both recommendations and fulfillment. Until those workflows have their own
contracts, keeping inventory under fulfillment makes the current ownership honest: the existing inventory outputs are
planning inputs and projections, not a general inventory service.

Warehousing is currently a fulfillment planning reference, not a separate operational domain. The
`schemas/fulfillment/warehouses/` package contains the tenant-scoped `Warehouse` relation used for active-facility
filtering, regional preference, and deterministic priority ordering. That is enough while a warehouse means “a place
the planner may select.” It should not yet become a top-level `warehousing/` package with only a facility dimension and
no independent transformations.

Promote warehousing to a sibling package when Store models facility operations independently—such as zones and
locations, capacity, labor or processing calendars, cutoff times, pick/pack capability, dock constraints, maintenance,
or warehouse-level service metrics. At that point, `schemas/warehousing/` and `transforms/warehousing/` can publish
facility capabilities and operational facts consumed by fulfillment. Keep the meanings separate: warehousing describes
where and how work can happen, inventory describes what stock exists, fulfillment decides how demand is served, and
shipping records what was dispatched or delivered.

Shipping should remain distinct from fulfillment planning. Fulfillment answers how a demand line should be served;
shipping represents execution and delivery evidence after that decision. The current `Shipment` relation is therefore
consumed by order publication, reconciliation, and service evaluation without creating a shipping package that has no
shipping-specific workflow yet. If Store later adds carrier-event normalization, package or split-shipment facts,
delivery milestones, tracking, returns, or carrier performance, those contracts should form a sibling
`transforms/shipping/` package. Fulfillment can consume its observed outputs for plan-versus-actual evaluation, while
shipping remains separate from both planning and the `orders/` publication facade.

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

`BuildPersonalizedRecommendations` is a separate workflow under `transforms/personalization/`. It adds normalized
catalog feature arrays, explicit customer category preferences, customer/session history, and a replaceable personal
algorithm. The workflow is tenant-scoped: `tenant_id` identifies the organization, `customer_id` identifies a known
shopper within that organization, `session_id` supports anonymous browsing, and `request_id` identifies one serving
request. The main `Recommender` joins the final personal rows by tenant, request, and product, then blends the personal
score into the existing behavior score. Empty personal inputs preserve the behavior-based fallback.

```python
recommendations = Recommender(
    requests=requests,
    catalog=catalog,
    policy=policy,
    boosts=boosts,
    suppressions=suppressions,
    session_events=session_events,
    fulfilled_orders=fulfilled_orders,
    feedback_impressions=feedback_impressions,
    feedback_clicks=feedback_clicks,
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

`BuildProductSignals` and `BuildPurchaseSignals` live under `recommender/signals/`. They turn
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

`Fulfillment` lives in the fulfillment transform package and captures the overall planning pipeline. Its `demand/`,
`inventory/`, `planning/`, `reconciliation/`, `shortages/`, and `substitutions/` subpackages mirror the business
phases. It composes `PrepareOrderDemand`, `PlanFulfillment`, `ReconcileFulfillmentPlan`, and `FulfillmentAnalytics`, exposing demand,
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
).run(session).recommended

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
