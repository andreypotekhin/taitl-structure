# Store Example Background

The Store example models a multi-tenant retail flow from product facts and recommendation serving through commercial
demand, fulfillment planning, shipment-backed actuals, and analytics. It is a collection of typed transformations, not
a hosted commerce platform: callers provide source relations, persistence, stream lifecycle, and the business actions
taken from the results.

The example keeps planned decisions, observed facts, and descriptive analytics separate. A recommendation can shape
demand before an order exists; an order can become valid demand without being allocated; a plan can be reconciled with a
later shipment; and an analytic summary can describe those facts without mutating any of them.

The executable source and fixture contract are the [Store example](../../examples/store/Readme.md) and its transforms
under `examples/store/transforms`. This background explains the business boundaries represented by those examples; it
does not add a production commerce-service contract.

## Tenant and Source Ownership

Store relations are tenant-scoped. A tenant identifies the organization whose products, customers, policies, warehouses,
orders, and feedback may be combined. Identifiers such as customer, product, order, and warehouse IDs are meaningful
only within their tenant unless a schema explicitly says otherwise.

Callers own source harvesting, reference-data freshness, persistence, inventory accuracy, and checkpointing. Stream
triggers, sinks, monitoring, privacy controls, and downstream actions remain caller-owned. Structure owns typed
transformations and generated artifacts.
It does not create orders, reserve inventory, change prices, send notifications, or dispatch shipments.

The Store fixtures are representative rather than exhaustive. They exercise multiple tenants, repeated product lines on
one order, inventory outcomes, recommendation inputs, and nullable lookup facts. Tests compare selected business rows
and columns instead of treating the fixture data as a complete retail dataset.

## Product Foundation

Product facts are prepared before recommendation or fulfillment paths consume them. `PrepareCatalog` keeps active,
tenant-visible products that are not blocked and enriches them with descriptive promotion evidence. A matching
product or category promotion can raise a promotion score; it does not override product eligibility.

`NormalizeCatalog` makes identifiers and category fields stable for downstream joins. `ExpandProductTaxonomy` derives
bounded ancestor facts so category-aware retrieval, ranking, and diversification can use the same tenant-scoped
hierarchy.
The taxonomy is a lookup and context boundary, not a recommendation decision by itself.

Catalog outputs are reusable inputs to both merchandising and fulfillment. They do not imply that a product is in stock,
available at a selected warehouse, or appropriate for a particular customer request.

## Recommendations and Merchandising

The merchandising path answers “which products should we show before demand exists?” Fulfillment planning answers “how
should we serve demand that already exists?” Keeping those questions separate prevents inventory and shipment facts from
silently changing recommendation meaning.

`Recommender` consumes recommendation requests, prepared catalog products, tenant-owned policy, boosts, suppressions,
and optional product and personal signals. Its transparent ranking preserves separate score components for eligibility,
promotion, policy boost, suppression penalty, inventory boost, and feedback. Final ordering is deterministic: higher
final score, lower suppression penalty, higher inventory boost, then product ID.

`BuildRecommendationCandidates` records candidate origin and context before ranking. Candidates may come from category
retrieval, session interest, or popularity evidence. `FilterRecommendationCandidates` records hard-suppression and
session-exclusion reasons rather than silently dropping every rejected row. `DiversifyRecommendations` applies the
taxonomy-branch cap after ranking, then `SelectRecommendedProducts` enforces the policy result limit.

`BuildPersonalizedRecommendations` is a separate workflow. It adds normalized catalog features, customer category
preferences, customer and session history, and a replaceable personal algorithm. The workflow is tenant-scoped:
`tenant_id` identifies the organization, `customer_id` identifies a known shopper within it, `session_id` supports
anonymous browsing, and `request_id` identifies one serving request. Empty personal inputs preserve the behavior-based
fallback.

Recommendation feedback remains caller-owned evidence. `BuildProductSignals` aggregates impressions and clicks,
`BuildSessionSignals` summarizes one-day session interests, and `BuildPurchaseSignals` attributes fulfilled order
products to recent recommendation impressions. A click counts only when it references an impression and occurs within
24 hours;
purchase attribution uses a 30-day impression boundary. These signals support ranking but are not relevance judgments or
causal experiment results.

`AssignRecommendationVariants` uses a stable tenant-scoped customer, session, or request key for assignment.
`EvaluateRecommendationExperiment` reports observed request, impression, click, purchase, and guardrail metrics for
served exposures. Stable assignment and observed comparisons do not establish causal impact without an explicit exposure
and selection-probability contract.

## Commercial Demand

`PrepareOrderDemand` turns incoming orders into commercially valid demand before warehouse or shipment decisions. It
reuses customer, product, blocked-product, and promotion checks, but stops before shipment matching. Its `Order` output
means that an order line is valid and ready for fulfillment planning; it does not mean that the line is allocated or
shipped.

Order lines retain `line_number` so repeated products on one order remain distinct. This identity is essential for
allocation, reconciliation, and service evaluation. A product name or product ID alone is not a safe line identity.

## Fulfillment Planning

`Fulfillment` composes the demand, inventory, planning, shortage, substitution, reconciliation, and analytics phases.
The main planning boundary consumes valid demand, active tenant-scoped warehouses, inventory positions, inbound
inventory, lead times, substitution rules, service targets, and later fulfilled facts.

`PlanFulfillment` computes available-to-promise as on-hand quantity minus reserved quantity, never below zero. Inbound
inventory does not increase immediate allocation; it can supply a possible planned ship date for a partial or full
backorder and inform a replenishment suggestion.

For each demand line, warehouse selection considers only active tenant-scoped options. The deterministic tie-breakers
are same customer region first, lower warehouse priority value, greater available-to-promise quantity, then warehouse
ID.
The first version does not split one order line across multiple warehouses.

`FulfillmentPlan.plan_status` is one of `allocated`, `partially_allocated`, or `backordered`. An order-date planned ship
date means the line can be allocated immediately. A later date comes from inbound inventory and is a planning signal,
not a guaranteed shipment promise. A null date means no inbound fact is known.

`ReplenishmentSuggestion` is descriptive. It appears when available-to-promise after planning falls below safety stock
and inbound inventory is late for the order date or absent. It does not create a purchase order, transfer, or
reservation.

`BuildDemandWindows` summarizes observed order demand by product and date; it does not forecast. `ProjectInventory`
applies on-hand, reservations, inbound receipts, and lead times across those windows. `DetectShortages` publishes the
first projected deficit, while `FindSubstitutions` ranks active tenant-scoped alternatives without rewriting the
original order line. `PrioritizeExceptions` exposes shortage, lateness, customer-tier, and service-target inputs rather
than hiding them in an opaque priority value.

Inventory is currently a fulfillment planning concern. A future independent inventory package would need its own
contracts, such as snapshot normalization, reservation reconciliation, availability publication, or inventory quality
checks consumed by more than one domain. Moving the current projection alone would rename a boundary without giving
inventory an independent purpose.

## Shipment-Backed Publication and Actuals

Shipping facts represent observed execution after planning. They must not be treated as evidence that a plan was
correct,
and a plan must not be presented as proof that a shipment occurred.

`EnrichOrders` can consume an orders stream with static customer, product, blocked-product, promotion, and shipment
lookups. It normalizes identifiers and text, defaults missing quantity, rejects invalid or negative commercial totals,
and publishes only order lines matched to observed shipment facts. Missing customer details can remain null through the
tenant-scoped left join; shipment matching is an inner join because publication represents fulfilled lines.

`ReconcileFulfillmentPlan` compares planned lines with later `OrderFulfillment` facts by tenant, order ID, and
`line_number`. It can establish that a line shipped; the current `Shipment` relation does not carry actual warehouse
identity, so reconciliation cannot claim that the planned warehouse performed the shipment.

`EvaluateFulfillment` reports line-safe on-time, in-full, lateness, and target-attainment results from observed shipment
dates. A missing shipment date stays unknown. Warehouse fields in service summaries refer to the planned warehouse, not
an inferred execution warehouse.

Shipping should become a separate domain only when Store admits shipping-specific workflows such as carrier-event
normalization, package or split-shipment facts, delivery milestones, tracking, returns, or carrier performance. Those
facts can feed fulfillment evaluation without placing shipping execution inside fulfillment planning.

## Analytics

`FulfillmentAnalytics` summarizes planning and warehouse load by tenant and business date. `OrderAnalytics` publishes
tenant-scoped customer-day totals, product-day summaries, and customer event rankings from fulfilled order lines.

Customer totals include order count and gross and net revenue. Product summaries add distinct customers, units, minimum,
maximum, average units, and gross revenue. Ranking outputs include row number, rank, dense rank, adjacent quantities,
and three-row rolling unit measures.

`AdvancedOrderAnalytics` demonstrates higher-level Spark expressions such as rollups, cubes, windows, collection
operations, approximate statistics, correlations, and grouping identifiers. Rollup and cube nulls represent subtotal
dimensions, not source values; consumers use the grouping metadata to distinguish them.

`RowsetJoinExamples` demonstrates full, right, and intentionally allowed Cartesian joins. It is a join-semantics
reference, not a recommendation for unbounded catalog expansion.

## Evidence and Policy Boundaries

Store keeps policy inputs visible. Recommendation weights, suppression rules, branch caps, service targets, warehouse
priority, safety stock, lead time, and promotion validity remain explicit relations or fields rather than hidden global
state.

Observed facts also retain their evidence boundary. Impressions, clicks, fulfilled orders, shipments, and event dates
are not interchangeable with plans, predictions, or judgments. A missing row may mean “not observed,” “not applicable,”
or “not yet available,” so each transform defines whether it preserves, filters, or summarizes that absence.

Tenant and line identities are part of the business contract. Joins must retain tenant scope, and order-level analysis
must retain `line_number` where repeated products can occur. Deterministic tie-breakers make generated results stable
without claiming that physical relation order is meaningful.

## Boundaries and Deferred Work

The Store example is intentionally not a storefront, order-management system, warehouse-control system, inventory
ledger, shipping service, payment processor, procurement system, customer messaging service, or production model host.
It does not own Spark sessions, streaming checkpoints, triggers, sinks, deployment, or recovery.

It also does not currently provide learned ranking, vector retrieval, causal experiment estimates, exploration controls,
returns and reverse logistics, dynamic order splitting, warehouse-aware execution facts, automated replenishment, price
optimization, fraud decisions, privacy governance, or model artifact promotion. Those additions need focused schemas,
evidence semantics, caller ownership, and direct generated or execution tests.

Keeping these boundaries explicit gives the example a small, inspectable foundation: tenant-scoped product facts,
transparent merchandising, commercially valid demand, deterministic fulfillment planning, shipment-backed actuals, and
descriptive analytics.
