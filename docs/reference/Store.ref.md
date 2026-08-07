# Store Example Reference

The Store example is a multi-tenant retail and fulfillment pipeline. Use it to locate transformations for catalog
facts, recommendations, commercial demand, fulfillment planning, shipment reconciliation, or analytics.

The Store background explains the evidence and policy boundaries. The Store example guide lists the executable
workflows and fixtures. Structure describes transformations; callers provide sources, persistence, business actions,
and streaming lifecycle.

This page describes the bundled Store example. Its product, order, tenant, and fulfillment names are example-app
schemas and transforms, not additional Structure core operations. Source declarations live under
`examples/store/schemas/` and `examples/store/transforms/`; the behavior remains defined here for offline use.

## Workflow map

| Stage | Typical transforms | Main result |
| --- | --- | --- |
| Product foundation | `PrepareCatalog`, `ExpandProductTaxonomy` | Tenant-scoped product facts |
| Recommendations | Candidate, ranking, and diversification transforms | Ranked, policy-filtered products |
| Demand admission | `PrepareOrderDemand` | Valid commercial `Order` lines |
| Fulfillment planning | Planning, windows, and inventory transforms | Plans and backorders |
| Exceptions | `DetectShortages`, `FindSubstitutions`, `PrioritizeExceptions` | Shortage and service-risk evidence |
| Actuals | `EnrichOrders`, `ReconcileFulfillmentPlan`, `EvaluateFulfillment` | Shipment facts and service metrics |
| Analytics | `FulfillmentAnalytics`, `OrderAnalytics`, `AdvancedOrderAnalytics` | Descriptive summaries and rankings |

Keep the workflow boundaries explicit when composing the example:

```python
catalog = PrepareCatalog(
    products=products,
    blocked_products=blocked_products,
    promotions=promotions,
).run(session).catalog
demand = PrepareOrderDemand(
    orders=raw_orders,
    customers=customers,
    products=products,
    blocked_products=blocked_products,
    promotions=promotions,
).run(session).demand
plan = PlanFulfillment(
    demand=demand,
    inventory_positions=inventory_positions,
    inbound_inventory=inbound_inventory,
    warehouses=warehouses,
).run(session).plans
reconciliation = ReconcileFulfillmentPlan(plans=plan, fulfilled=fulfilled).run(session).reconciliation
```

Each result has a distinct grain and business meaning; passing a plan to a shipment action would cross a boundary the
example does not own.

## Product foundation

`PrepareCatalog` creates stable tenant-scoped product facts. `ExpandProductTaxonomy` derives
bounded ancestor facts for category-aware retrieval and recommendation.

Product identity normally includes `(tenant_id, product_id)`. A join on `product_id` alone is incomplete even when a
fixture happens to use globally unique IDs. Keep tenant scope in every lookup and output projection.

Use separate predicates for eligibility and enrichment:

```python
left_join(on=(catalog.tenant_id == policy.tenant_id) & (catalog.product_id == policy.product_id))
where(~exists(on=catalog.product_id == blocked.product_id))

return CatalogProduct.project(catalog)(
    tenant_id=catalog.tenant_id,
    product_id=catalog.product_id,
    product_name=catalog.name,
    category=catalog.category,
    promotion_name=promotion.name,
)
```

An absent promotion can leave nullable enrichment without removing an eligible product. A blocked product is excluded
because blocking is a policy predicate.

## Recommendations

The recommendation path answers which products to show before demand exists. `BuildRecommendationCandidates` records
candidate origin and context; `FilterRecommendationCandidates` records suppression and exclusion reasons;
`Recommender` and `Merchandising` calculate transparent score components; `DiversifyRecommendations` applies taxonomy
branch caps; `SelectRecommendedProducts` applies the result limit.

```python
signals = BuildRecommendationSignals(
    session_events=session_events,
    fulfilled_orders=fulfilled_orders,
    impressions=impressions,
    clicks=clicks,
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

diverse = DiversifyRecommendations(ranked=ranked.ranked_candidates, policy=policy).run(session)
published = SelectRecommendedProducts(ranked_candidates=diverse.diversified).run(session)
```

Final ordering is deterministic: higher final score, lower suppression penalty, higher inventory boost, then product
ID. A request is not a customer, and a session is not necessarily an authenticated customer. Empty personal inputs
preserve the behavior-based fallback.

Recommendation impressions, clicks, and purchases are evidence used by signals. A click is counted only when it
references an impression within the documented window; a purchase uses the documented impression boundary. These facts
support ranking but are not relevance judgments or causal experiment results.

## Commercial demand

`PrepareOrderDemand` converts incoming order rows into valid commercial demand before allocation or shipment decisions.
It cleans identifiers, validates required fields and monetary values, performs tenant-scoped lookups, and selects a
promotion valid at the order date.

Order-line identity includes `line_number`. Repeated products on one order remain distinct lines and must not be merged
by product ID alone.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    where(order.customer_id.is_not_null())
    where(order.product_id.is_not_null())
    return OrderNormalized.project(order)(
        id=self.clean_id(order.id),
        customer_id=self.clean_id(order.customer_id),
        product_id=self.clean_id(order.product_id),
        net_total=(self.money(order.total) - self.money(order.discount)).cast(types.decimal(12, 2)),
        quantity=coalesce(order.quantity, 1),
    )
```

Demand means a valid order line ready for planning. It does not mean allocated, shipped, or promised.

## Fulfillment planning

`PlanFulfillment` consumes valid demand, active tenant-scoped warehouses, inventory, inbound receipts, lead times,
substitution rules, and service targets. Available-to-promise is on-hand quantity minus reserved quantity, never below
zero. Inbound inventory can inform a planned ship date but does not increase immediate allocation.

For each line, warehouse selection prefers the customer region, then lower warehouse priority, greater available stock,
and warehouse ID. The current model does not split one order line across multiple warehouses.

| Status | Meaning |
| --- | --- |
| `allocated` | The selected warehouse can serve the requested quantity now |
| `partially_allocated` | The plan can serve part of the requested quantity |
| `backordered` | No complete immediate allocation is available |

`ReplenishmentSuggestion` is descriptive. It does not create a purchase order, transfer, reservation, or shipment
promise. `BuildDemandWindows` summarizes observed demand; it does not forecast. `ProjectInventory` projects supplied
facts across windows; it does not create an inventory ledger.

```python
plans = PlanFulfillment(
    demand=demand,
    inventory_positions=inventory_positions,
    inbound_inventory=inbound,
    warehouses=warehouses,
).run(session)

backorders = plans.plans.where(plans.plans.status == "backordered")
```

The plan reports a decision from supplied facts. A caller must separately decide whether to reserve, purchase, or
communicate the result.

## Shipment and reconciliation

Shipment facts represent observed execution after planning. They are not proof that the plan was correct, and a plan is
not proof that a shipment occurred.

`ReconcileFulfillmentPlan` matches by tenant, order ID, and `line_number`. It is a left reconciliation so an unshipped
or not-yet-observed line remains visible:

```python
left_join(
    on=(plan.tenant_id == shipment.tenant_id)
    & (plan.order_id == shipment.id)
    & (plan.line_number == shipment.line_number)
)
return FulfillmentReconciliation.project(plan)(
    shipped_quantity=sum(coalesce(shipment.quantity, 0)),
    shipped=bool_or(shipment.id.is_not_null()),
)
```

If a source replays shipment events, the caller must establish whether rows are immutable events or latest state before
feeding them to reconciliation. Store does not deduplicate arbitrary shipment events automatically.

## Analytics

Choose the grain before choosing the aggregate:

```python
group_by(
    tenant_id=order.tenant_id,
    customer_id=order.customer_id,
    business_date=order.business_date,
)

return CustomerDailyTotal.project(order)(
    order_count=count_distinct(order.id),
    gross_revenue=sum(order.total),
    net_revenue=sum(order.net_total),
)
```

Grouped summaries replace source rows. Window rankings retain them. Rollups and cubes add subtotal rows whose null
dimensions are structural; use grouping metadata before interpreting them as missing source values.

| Output | Grain | Meaning |
| --- | --- | --- |
| `CatalogProduct` | tenant and product | Eligible product fact |
| `RecommendedProduct` | request and product | Served recommendation |
| `Order` | order line | Valid commercial demand |
| `FulfillmentPlan` | order line | Planned fulfillment decision |
| `FulfillmentReconciliation` | planned order line | Comparison with observed shipment |
| Daily summaries | Declared business grain | Descriptive aggregate |

## Boundaries

The example is not a storefront, order-management system, warehouse-control system, inventory ledger, shipping
service, payment processor, procurement system, model host, or production orchestration layer. It does not own Spark
sessions, checkpoints, triggers, sinks, or recovery.

It also does not provide learned ranking, vector retrieval, causal experiment estimates, dynamic order splitting,
automated replenishment, price optimization, fraud decisions, returns, or privacy governance. Those additions require
new focused schemas, evidence semantics, and runtime tests.

```python
plan = PlanFulfillment(
    demand=demand,
    warehouses=warehouses,
    inventory_positions=inventory_positions,
    inbound_inventory=inbound_inventory,
).run(session)

# A plan is data for an application action, not the action itself.
plan.plans.write.mode("overwrite").parquet(plan_path)
```

## Stable grains

| Relation family | Identity or grain |
| --- | --- |
| Catalog | Tenant and product |
| Recommendation request | Tenant, request, and customer/session context |
| Recommendation result | Request and product |
| Commercial demand | Tenant, order, and `line_number` |
| Fulfillment plan | One row per demand line in the current model |
| Shipment fact | Tenant, order, and `line_number` plus source event/state contract |
| Analytics | Explicit customer-day, product-day, warehouse-day, or other declared grain |

`line_number` is essential when one order contains the same product more than once. Product ID alone is not a safe
reconciliation key. Every cross-tenant lookup should include tenant identity even when a fixture does not demonstrate a
collision.

Catalog normalization should publish the smallest stable product contract needed by recommendation and demand flows.
Do not carry shipment status, allocation, or a fulfillment promise merely because those fields were available during a
catalog join. Separate eligibility from descriptive enrichment so missing optional facts remain distinguishable from
policy exclusion.

```python
return CatalogProduct.project(product)(
    tenant_id=product.tenant.tenant_id,
    product_id=product.id,
    product_name=product.name,
    category=product.category,
)
```

The catalog result stays at product grain; fulfillment and shipment fields belong to later relations.

## Recommendation policy inputs

Recommendation policy remains visible in caller-supplied relations or fields:

| Input | Use |
| --- | --- |
| Candidate origin | Explain category, session, popularity, or other admission source |
| Suppression | Exclude or penalize a product with a recorded reason |
| Boost | Add a transparent policy adjustment |
| Inventory signal | Supply a ranking feature, not a reservation |
| Taxonomy branch | Apply deterministic diversity caps after ranking |
| Result limit | Bound the published request result |

`AssignRecommendationVariants` uses a stable tenant-scoped customer, session, or request key. It does not claim
randomized causal assignment without a separate exposure contract. `EvaluateRecommendationExperiment` reports observed
request, impression, click, purchase, and guardrail metrics for served exposures.

The recommendation path should preserve intermediate relations when diagnosing an absent product. A product may have
failed eligibility, never entered candidates, been suppressed, lost on score, or been removed by diversification; a
single final absence cannot distinguish those cases.

```python
candidates = BuildRecommendationCandidates(
    requests=requests,
    catalog=catalog,
    taxonomy=taxonomy,
    session_features=session_features,
    signals=recommendation_signals,
    suppressions=suppressions,
).run(session)
filtered = FilterRecommendationCandidates(
    candidates=candidates.candidates,
    suppressions=suppressions,
).run(session)
```

Inspect `candidates` and `filtered` before the final selection when explaining why a product disappeared.

## Demand semantics

`PrepareOrderDemand` uses a temporal promotion lookup rather than a current-value lookup. A promotion is selected when
it is valid at the order's business date; overlapping validity should be an explicit source error or selection policy.
Customer descriptions can remain nullable through a left join, while a blocked product is rejected with an existence
predicate. This separates “valid demand with missing enrichment” from “demand excluded by policy.”

The normalized intermediate Schema is useful even when it is not a public output. It makes cleaned identifiers,
monetary conversions, null repair, and required-key validation visible to later planning steps.

```python
demand = PrepareOrderDemand(
    orders=raw_orders,
    customers=customers,
    products=products,
    blocked_products=blocked_products,
    promotions=promotions,
).run(session).demand
```

Pass the normalized demand relation to planning; do not treat raw order input as already valid commercial demand.

## Planning decisions

| Decision | Contract |
| --- | --- |
| Available-to-promise | `max(on_hand - reserved, 0)` |
| Immediate allocation | Uses current available quantity only |
| Inbound inventory | May inform a possible planned date, not immediate stock |
| Warehouse choice | Region, priority, available quantity, then ID tie-breakers |
| Split allocation | Not part of the current one-plan-row-per-line model |
| Replenishment | Descriptive signal below safety stock, not an external action |

If future split allocation is added, it needs a new allocation grain and quantity-conservation rules. It must not
silently change the meaning of `FulfillmentPlan`.

`DetectShortages` publishes the first projected deficit. `FindSubstitutions` ranks active tenant-scoped alternatives
without rewriting the original order line. `PrioritizeExceptions` keeps shortage, lateness, customer tier, and service
targets visible rather than hiding them in an opaque priority value.

```python
shortages = DetectShortages(projections=inventory_projections).run(session)
substitutions = FindSubstitutions(
    demand=demand,
    rules=substitution_rules,
    inventory_positions=inventory_positions,
).run(session)
exceptions = PrioritizeExceptions(
    shortages=shortages.shortages,
    plans=plans.plans,
    demand=demand,
    substitutions=substitutions.options,
    service_targets=service_targets,
).run(session)
```

These relations support an operational decision; they do not perform the decision themselves.

## Shipment-backed actuals

`EvaluateFulfillment` reports on-time, in-full, lateness, and target-attainment results from observed shipment dates. A
missing shipment date stays unknown; it is not automatically late. Warehouse fields in service summaries identify the
planned warehouse unless the shipment source carries an explicit execution warehouse.

Publication is intentionally different from planning: a plan can exist without a shipment, and a shipment can expose a
planning mismatch. Keep those facts in separate relations so downstream consumers can distinguish “planned,”
“observed,” and “reconciled.”

```python
evaluation = EvaluateFulfillment(
    plans=plans.plans,
    fulfilled=fulfilled,
    service_targets=service_targets,
).run(session)
```

Evaluate observed shipment evidence after reconciliation; do not infer shipment performance from the plan alone.

## Analytics details

`FulfillmentAnalytics` summarizes planning and warehouse load by tenant and business date. `OrderAnalytics` publishes
customer-day totals, product-day summaries, and customer event rankings from fulfilled lines. Product summaries can
include distinct customers, units, min/max/average units, and gross revenue. Ranking outputs retain row identity and
add row-number, rank, dense-rank, adjacent quantities, or bounded rolling measures.

`AdvancedOrderAnalytics` demonstrates rollups, cubes, windows, collections, approximate statistics, correlations, and
grouping identifiers. It is an analytical example, not a replacement for the smaller business-grain transforms.

```python
analytics = AdvancedOrderAnalytics(
    orders=fulfilled_orders,
    products=catalog,
).run(session)

customer_rank = analytics.customer_rank.orderBy("tenant_id", "rank")
```

The caller chooses persistence and presentation of the summary; the transform keeps the declared analytic grain.

## Before using Store results

- Include tenant scope in every cross-tenant join.
- Preserve `line_number` through demand, planning, shipment, and reconciliation.
- Keep policy inputs visible and caller-supplied.
- Distinguish candidate, recommendation, demand, plan, shipment, and evaluation grains.
- Treat absent enrichment, policy exclusion, and unobserved actuals as different meanings.
- Use deterministic tie-breakers without implying that physical DataFrame order is meaningful.
- Keep source persistence, business actions, and streaming lifecycle outside the transforms.

## Evidence vocabulary

Use the following distinctions when reading Store outputs:

| Term | Meaning |
| --- | --- |
| Eligible | A product or line passed the current policy predicates |
| Candidate | A row admitted for recommendation or fulfillment consideration |
| Recommendation | A product selected for one request, not a purchase |
| Demand | A commercially valid order line, not an allocation |
| Plan | A proposed warehouse/quantity/date decision, not a reservation |
| Actual | An observed shipment or fulfillment fact |
| Reconciliation | A comparison between a plan and later actual evidence |
| Summary | A descriptive aggregate at an explicitly chosen grain |

The same absence can have different meanings at each boundary: no eligible product, no candidate, suppressed result, no
allocation, not-yet-observed shipment, or no contributing facts. Preserve the relation and policy context needed to
interpret it rather than replacing every absence with a zero or dropping it through an inner join.

## Recommended execution order

For a demand-to-service workflow, keep the semantic order visible:

```text
tenant-scoped catalog facts
  -> valid commercial demand
  -> eligible warehouse and inventory facts
  -> fulfillment plan and shortage evidence
  -> shipment-backed reconciliation
  -> service and warehouse summaries
```

For recommendation serving, keep a separate path:

```text
catalog and taxonomy
  -> request-scoped candidates
  -> suppression and policy filtering
  -> transparent ranking
  -> taxonomy diversification
  -> bounded publication
```

Do not let shipment facts silently change recommendation eligibility, or let a plan be presented as evidence of a
shipment. These are separate transformations and separate business claims.

## Choosing a Store output

- Does every join preserve tenant and line identity?
- Does every output name its grain and absence meaning?
- Are recommendation policy, service targets, warehouse priority, and safety stock visible inputs?
- Are deterministic tie-breakers explicit after each ranking or selection?
- Does a nullable lookup remain nullable rather than filtering valid demand?
- Are descriptive projections kept separate from actions such as reservation, purchase, or publication?
- Would a replayed shipment event inflate quantity or contradict a latest-state source?
- Are future extensions adding a new contract instead of quietly changing an existing relation's grain?

## Compatibility and lifecycle

Store transforms are batch-oriented examples unless a particular transform declares and passes the supported streaming
contract. The caller supplies source DataFrames, Spark sessions, persistence, business actions, checkpoints, triggers,
and sinks. A `FulfillmentPlan` does not reserve stock, `RecommendedProduct` does not publish a storefront response, and
`FulfillmentReconciliation` does not mutate a shipment ledger.

Generated and online execution should preserve the same tenant keys, line identity, policy decisions, nullability, and
result grain. If a target-specific hook is needed for an operational action, keep it at an explicit hook boundary and
document who performs the action and how retries behave.

```python
result = Fulfillment(
    orders=orders,
    customers=customers,
    products=products,
    warehouses=warehouses,
    inventory_positions=inventory_positions,
).run(session)

# The caller controls persistence and any retry/idempotency policy.
result.plans.write.mode("overwrite").parquet(plan_path)
```

## Extension boundaries

Potential future features should add focused contracts rather than overload existing outputs:

| Future concern | Needed new boundary |
| --- | --- |
| Split allocation | Allocation rows and quantity conservation |
| Carrier execution | Shipment events, packages, delivery milestones, and correction policy |
| Automated replenishment | Purchase/transfer intent and external idempotence |
| Learned recommendation | Model artifact, feature snapshot, and evaluation contract |
| Returns | Reverse-flow line identity and inventory disposition |
| Privacy/fraud | Access, retention, and decision-audit contracts |

The current example remains transparent because each new business claim needs a matching Schema, evidence source,
caller action, and testable failure boundary.

Treat this record as part of the business contract when a Store relation crosses a domain boundary or is persisted for
later reconciliation.

That record also makes generated-code changes reviewable when a source edit changes a join, tie-breaker, or output
grain.

Do not treat a Store reference result as an inventory reservation, purchase order, shipment command, or accounting
posting. Those actions need their own idempotency, authorization, and reconciliation contracts outside the transform
graph.

## Related concepts

The Store background explains tenant identity, planned-versus-observed facts, and policy boundaries. The Store
example guide groups the focused boundary specifications. Transform, join, and aggregation references describe
general Structure vocabulary; this page applies that vocabulary to Store without requiring those documents.
