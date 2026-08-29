# Store

The Store example models a multi-tenant retail flow from product facts and recommendations through commercial demand,
fulfillment planning, shipment-backed actuals, and analytics. It is a collection of typed transformations, not a hosted
commerce platform. Callers provide source relations, persistence, stream lifecycle, and business actions.

The example keeps planned decisions, observed facts, and descriptive analytics separate. A recommendation can shape
demand before an order exists; an order can become valid demand without being allocated; a plan can be reconciled with a
later shipment; and an analytic summary can describe those facts without mutating any of them.

The executable source and fixture contract are the Store example under `examples/store/`, with transforms under
`examples/store/transforms/`. The business boundaries are typed relations whose tenant identity, grain, timestamp,
and planned-versus-observed state remain explicit. This page explains those boundaries; it does not add a
production commerce-service contract.

## Store Flow

The example has several connected evidence flows. Each flow has a distinct grain and a different answer it can provide:

```text
product facts ──> prepared catalog ──> recommendation candidates ──> served products
                                      │                                  │
                                      └──── policies, signals, feedback ┘

raw orders ──> valid demand ──> fulfillment plan ──> reconciliation with shipment facts
                    │                │                         │
                    └──── demand windows and projections ──────┘

fulfilled order lines ──> customer/product/warehouse analytics
```

The arrows describe data dependencies, not side effects. A transform publishes a typed relation or artifact; the
caller decides whether to persist it, expose it to another transform, or use it to trigger an external action.

The primary reading order is:

1. Establish tenant, identity, and source ownership.
2. Prepare product and order facts without losing the business grain.
3. Rank recommendations and plan fulfillment as separate decisions.
4. Compare plans with observed shipment facts.
5. Summarize the evidence with explicitly chosen grouping and time semantics.

This order matters. For example, a recommendation impression is evidence that a product was served, while an order
line is evidence of demand, and a fulfillment plan is a decision about how that demand might be served. Combining
those rows prematurely makes later attribution and evaluation ambiguous.

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

The product preparation boundary illustrates the intended shape: normalize the identity, apply hard eligibility checks,
then project a compact relation for downstream consumers.

```python
@transform
class PrepareCatalog(Transform):
    products = input(Product)
    blocked_products = input(BlockedProduct)
    catalog = output(CatalogProduct)

    @step(output=catalog)
    def prepare(self, product: Product, blocked: BlockedProduct) -> CatalogProduct:
        where(product.active)
        where(
            not_exists(
                on=(blocked.tenant.tenant_id == product.tenant.tenant_id)
                & (blocked.id == product.id)
            )
        )
        return CatalogProduct.project(product)(
            product_id=product.id,
            product_name=product.name,
            category=product.category,
        )
```

The exact Store implementation also considers promotions and taxonomy. The important design property is the
separation between an eligibility predicate and an enrichment projection. An absent promotion can leave enrichment
nullable without making an otherwise eligible product disappear; a blocked product is excluded because blocking is a
policy predicate.

Product identity is carried through every later stage. If a catalog row is tenant-scoped by `(tenant_id, product_id)`,
then a join on `product_id` alone is incomplete even when a fixture happens to use globally unique IDs. This rule is
especially important for shared infrastructure, backfills, and multi-tenant replay.

## Recommendations and Merchandising

The merchandising path answers “which products should we show before demand exists?” Fulfillment planning answers
“how should we serve demand that already exists?” Keeping those questions separate prevents inventory and shipment
facts from silently changing recommendation meaning.

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

The recommendation workflow makes the stages visible rather than hiding them in one ranking expression:

```python
signals = BuildRecommendationSignals(
    session_events=session_events,
    fulfilled_orders=fulfilled_orders,
    impressions=feedback_impressions,
    clicks=feedback_clicks,
)

candidates = BuildRecommendationCandidates(
    requests=requests,
    catalog=catalog,
    taxonomy=taxonomy,
    session_features=signals.session_features,
    signals=signals.recommendation_signals,
    suppressions=suppressions,
)

ranked = RankRecommendationCandidates(
    candidates=candidates.candidates,
    policy=policy,
    boosts=boosts,
    suppressions=suppressions,
    signals=signals.recommendation_signals,
    ranker=ranker,
)

diversified = DiversifyRecommendations(ranked=ranked.ranked_candidates, policy=policy)
published = SelectRecommendedProducts(ranked_candidates=diversified.diversified)
```

Each intermediate relation can be inspected at its own grain. Candidates explain why a product entered the set;
ranking explains score components; diversification applies branch-level policy; publication applies the result limit.
This gives callers useful diagnostics when a product is absent: it may have failed eligibility, never entered the
candidate set, been suppressed, lost on score, or been removed by a diversity cap.

A request is not a customer and a session is not necessarily an authenticated customer. Anonymous session signals may
support a request while customer-specific preferences remain absent. The workflow therefore preserves request and
session identity independently and treats an empty preference relation as a valid fallback case.

## Commercial Demand

`PrepareOrderDemand` turns incoming orders into commercially valid demand before warehouse or shipment decisions. It
reuses customer, product, blocked-product, and promotion checks, but stops before shipment matching. Its `Order` output
means that an order line is valid and ready for fulfillment planning; it does not mean that the line is allocated or
shipped.

Order lines retain `line_number` so repeated products on one order remain distinct. This identity is essential for
allocation, reconciliation, and service evaluation. A product name or product ID alone is not a safe line identity.

The demand transform is deliberately staged. It cleans and validates the raw line first, preserves a normalized
intermediate type, then performs tenant-scoped lookups and a temporal promotion lookup before publishing demand.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    where(order.customer_id.is_not_null())
    where(order.product_id.is_not_null())
    total = self.money(order.total)
    discount = self.money(order.discount)
    return OrderNormalized.project(order)(
        id=self.clean_id(order.id),
        customer_id=self.clean_id(order.customer_id),
        product_id=self.clean_id(order.product_id),
        total=total,
        discount=discount,
        net_total=(total - discount).cast(types.decimal(12, 2)),
        quantity=coalesce(order.quantity, 1),
    )

def add_promotion(self, order: OrderWithProduct, promotion: Promotion) -> OrderWithPromotion:
    temporal_one(
        on=(promotion.tenant.tenant_id == order.tenant.tenant_id)
        & self.clean_id(promotion.code).null_safe_eq(order.promotion_code),
        at=order.business.order_date,
        valid_from=promotion.valid_from,
        valid_to=promotion.valid_to,
        how="left",
    )
    return OrderWithPromotion.project(order)(promotion_name=promotion.name)
```

The normalized type is useful even when it is not a public output: it makes the contract after validation explicit and
prevents later stages from accidentally reinterpreting raw strings. `temporal_one` expresses “the promotion valid at
the order date,” which is different from a current lookup. `not_exists` rejects blocked products while the customer
lookup remains a left join, so missing descriptive customer details do not erase otherwise valid demand.

The final demand projection should expose the smallest stable contract needed by planning. It should not silently carry
shipment status, allocation, or a fulfillment promise merely because those columns were available during preparation.

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

Planning can be read as a constrained row decision:

```text
valid demand line
  -> eligible warehouse candidates
  -> available-to-promise calculation
  -> deterministic warehouse choice
  -> allocation or backorder outcome
  -> optional inbound date and replenishment evidence
```

The result is intentionally one plan row per demand line in the current model. That keeps reconciliation and service
evaluation line-safe. A future split-allocation design would need a new allocation grain, such as one row per
`(tenant_id, order_id, line_number, warehouse_id)`, plus explicit quantity conservation rules. It should not quietly
change the meaning of the current `FulfillmentPlan` relation.

For example, the core availability calculation is a business expression rather than an external mutable reservation:

```python
raw_available = (
    coalesce(inventory.on_hand_quantity, 0)
    - coalesce(inventory.reserved_quantity, 0)
)
available = when(raw_available > 0, raw_available).otherwise(0)

plan_status = when(available >= demand.requested_quantity, "allocated").otherwise("backordered")
```

The Store implementation adds partial allocation, warehouse preference, inbound timing, and replenishment evidence.
The example above is therefore a semantic kernel, not a replacement for the full planner. The distinction matters for
review: an expression can calculate a candidate quantity, but it cannot by itself reserve stock or guarantee a ship
date.

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

The reconciliation contract is intentionally small and explicit:

```python
reconcile_key = (
    (plan.tenant.tenant_id == shipment.tenant.tenant_id)
    & (plan.order_id == shipment.id)
    & (plan.line_number == shipment.line_number)
)

left_join(on=reconcile_key)

return FulfillmentReconciliation.project(plan)(
    shipped_quantity=sum(coalesce(shipment.quantity, 0)),
    shipped=bool_or(shipment.id.is_not_null()),
)
```

This is a left reconciliation because an unshipped or not-yet-observed line is itself useful output. An inner join
would turn “not observed” into “not present,” making backorders and late shipments disappear from evaluation.
The actual implementation preserves the planned status and computes on-time and in-full measures with explicit null
handling.

When a source can emit corrections, the source contract must define whether a shipment row is an immutable event or a
latest state. The Store transforms do not deduplicate arbitrary shipment events on their own. A caller that receives
replays must establish the event/state contract before feeding actuals into reconciliation; otherwise repeated facts can
inflate quantities or produce contradictory dates.

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

Analytics starts by choosing the question's grain. The same fulfilled lines can support different, non-interchangeable
outputs:

```python
group_by(
    tenant_id=order.tenant.tenant_id,
    customer_id=order.customer_id,
    business_date=order.business.order_date,
)

return CustomerDailyTotal.project(order)(
    order_count=count_distinct(order.id),
    gross_revenue=sum(order.total),
    net_revenue=sum(order.net_total),
)
```

This is a customer-day summary, not a customer lifetime total and not an order-level projection. A product-day output
uses a different grouping key; a customer event rank preserves event rows and adds window values instead of collapsing
them. Choosing the grain first prevents an aggregate from accidentally changing the meaning of a downstream join.

The advanced analytics path is useful when the business question genuinely needs analytical structure:

```python
group_by(
    tenant_id=order.tenant.tenant_id,
    product_id=order.product_id,
    business_date=order.business.order_date,
)

return ProductDailySummary.project(order)(
    units=sum(order.quantity),
    distinct_customers=count_distinct(order.customer_id),
    average_units=avg(order.quantity),
    min_units=min(order.quantity),
    max_units=max(order.quantity),
)
```

Rollups and cubes add subtotal rows whose null dimensions are structural. A consumer must inspect grouping metadata
before interpreting a null category as a missing source category. Window functions retain row identity, while grouped
aggregates replace many source rows with one summary row; these are different contracts even when they calculate a
similar total.

For a compact review, Store's major outputs can be classified as follows:

| Output | Grain | Evidence or decision | Typical absence meaning |
| --- | --- | --- | --- |
| `CatalogProduct` | tenant and product | eligible product fact | not eligible or not observed |
| `RecommendedProduct` | request and product | served recommendation | not selected or suppressed |
| `Order` | order line | valid commercial demand | invalid or rejected line |
| `FulfillmentPlan` | order line | planned fulfillment decision | no eligible allocation |
| `FulfillmentReconciliation` | planned order line | shipment comparison | not yet observed as shipped |
| daily summaries | chosen business grain | descriptive aggregate | no contributing facts |

The table is a navigation aid, not a replacement for each schema. The same field can be nullable for different reasons,
so transforms must preserve the distinction between “no matching lookup,” “filtered by policy,” and
“no observed event.”

## Evidence and Policy Boundaries

Store keeps policy inputs visible. Recommendation weights, suppression rules, branch caps, service targets, warehouse
priority, safety stock, lead time, and promotion validity remain explicit relations or fields rather than hidden global
state.

Observed facts also retain their evidence boundary. Impressions, clicks, fulfilled orders, shipments, and event dates
are not interchangeable with plans, predictions, or judgments. A missing row may mean “not observed,”
“not applicable,” or “not yet available,” so each transform defines whether it preserves, filters, or summarizes
that absence.

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
