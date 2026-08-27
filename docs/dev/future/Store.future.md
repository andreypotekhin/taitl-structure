# Store app future

This document records Store capabilities that are plausible future admissions but are not part of the currently
admitted Store application. It is a design backlog, not a promise that every item will be implemented. A capability
becomes admitted only after it has a focused typed contract, a public transform boundary, generated-code evidence,
online/generated parity where applicable, differential or integration tests, documentation, and an explicit ownership
decision for sources, persistence, orchestration, and side effects.

The current Store application already includes catalog preparation, taxonomy expansion, recommendation serving,
personalization, session and purchase signals, recommendation evaluation and experiments, demand preparation,
warehouse allocation, inventory projection, shortages, substitutions, exceptions, fulfillment evaluation, order
enrichment, shipment-backed publication, and analytical summaries. The items below are intentionally beyond that
current scope.

## Demand and supply planning

### Forecasted demand

The current `BuildDemandWindows` path summarizes observed order demand; it does not forecast future demand. A future
`ForecastDemand` workflow could produce versioned forecasts by tenant, product, warehouse, category, and date window,
with explicit forecast horizon, confidence bounds, missing-history behavior, and model version. The forecast must remain
separate from observed demand and must not silently alter fulfillment allocation.

### Multi-echelon inventory planning

Current inventory projection is warehouse-local. A future network planner could model suppliers, distribution centers,
retail locations, transfers, purchase orders, minimum order quantities, vendor lead times, and transfer capacity. It
could emit transfer and purchase recommendations while keeping recommendations separate from executed receipts and
shipments. This should be admitted only with explicit network topology and temporal inventory contracts; a collection of
warehouse joins is not sufficient.

### Replenishment optimization

Current `ReplenishmentSuggestion` is descriptive and safety-stock based. A future `PlanReplenishment` workflow could
consider forecast demand, order costs, carrying costs, service targets, order calendars, and supplier constraints. It
should produce explainable proposals rather than issue purchase orders. Automated procurement and supplier side effects
remain caller-owned.

### Dynamic promise dates and order splitting

The current planner selects one preferred warehouse and does not model a full promise calendar. A future workflow could
calculate date-specific available-to-promise quantities, split an order line across warehouses, choose a split-shipment
policy, and publish promised-versus-planned dates. This needs a precise contract for reservation consumption, allocation
ordering across competing orders, calendar and cutoff times, and whether a promise is advisory or binding.

## Fulfillment execution and service

### Warehouse-aware actual execution

`Shipment` currently lacks warehouse identity, so reconciliation can establish that a line shipped but not its source
warehouse. A future `Shipment` extension or separate execution fact could add
warehouse, package, pick, pack, carrier handoff, and delivery events. The implementation must preserve the distinction
between planned allocation and observed execution and must define duplicate, correction, cancellation, and late-event
semantics.

### Returns and reverse logistics

Store has forward order and shipment behavior but no returns, refunds, exchanges, restocking, or disposition facts. A
future `Returns` workflow could attribute returns to order lines, classify reasons and product condition, calculate
refundable amounts, update inventory disposition, and report reverse-logistics service metrics. Financial settlement,
payment capture, and inventory writes would remain caller-owned boundaries.

### Carrier and delivery performance

A future delivery workflow could normalize carrier events, derive promised/actual milestones, detect late or
lost packages, and summarize carrier performance. It should operate over caller-provided shipment events and expose
unknown or contradictory milestones rather than infer them from missing rows.

### Fulfillment cost and margin

Current Store reports revenue and unit measures but does not allocate fulfillment cost. A future cost pipeline could
combine handling, packaging, shipping, discounts, returns, and product cost to derive margin across order, product,
warehouse, and strategy. Cost facts must retain their source and effective date;
the example should not present estimated cost as accounting truth.

## Merchandising and recommendation quality

### Learned ranking and candidate retrieval

The current recommender deliberately uses transparent policy, promotion, inventory, personalization, and feedback
components. A future learned-ranking path could train and serve a versioned model artifact, support collaborative or
content-based candidate generation, and compare it with the transparent baseline. Admission requires feature-contract
validation, deterministic snapshots, artifact compatibility, fallback behavior, and offline/online evaluation. Embedding
or vector retrieval should be a separate candidate-generation boundary, not hidden inside the
existing ranker.

### Causal experiment evaluation

Current recommendation experiments report observed variant behavior. A future evaluator could use randomized exposure
probabilities, treatment ownership, guardrail metrics, confidence intervals, and counterfactual or causal estimates.
Stable assignment alone is not enough: the serving caller must log exposure and selection probabilities that support the
claimed estimator. Without that evidence, Store should continue to label results as descriptive comparisons.

### Exploration and feedback-loop controls

Recommendations currently consume historical feedback as a ranking signal. A future exploration workflow could reserve a
controlled share of traffic for under-exposed products, clip or calibrate propensity weights, detect bot and accidental
click traffic, monitor popularity feedback loops, and enforce exposure floors or ceilings. These controls require trust,
identity, policy-version, and monitoring contracts beyond daily click aggregates.

### Richer commerce events

The current feedback path emphasizes impressions, clicks, and fulfilled purchases. Future admissions could add product
detail views, add-to-cart events, checkout starts, cancellations, subscriptions, wish-list actions, and explicit hides.
Each event needs an attribution window, deduplication key, request or session relationship, and a clear distinction
between observed behavior and business outcome. Adding event names without those semantics would make recommendation
signals ambiguous.

### Pricing and promotion optimization

Store currently treats promotions as descriptive ranking evidence. A future pricing and promotion workflow could model
price history, eligibility, markdown schedules, margin floors, budget limits, and promotion cannibalization. It should
recommend policy changes for caller approval, not mutate prices or campaigns. Optimization claims would require an
explicit objective and experiment or evaluation contract.

## Customer and commerce operations

### Customer lifecycle and loyalty

A future customer workflow could derive lifecycle states, retention cohorts, loyalty balances, tier transitions, and
customer-value summaries from order and engagement facts. Identity resolution, privacy retention, consent, and financial
ledger ownership must remain explicit. Anonymous session identifiers must not be silently merged with known customers.

### Fraud and order risk

Fraud and abuse detection is a natural Store-adjacent extension: payment risk, takeover signals, promotion abuse,
refund abuse, and bot behavior could be normalized into explainable risk evidence. A future workflow should publish
decision facts and reason codes, not approve or reject payments. Security-sensitive identity and trust inputs need
a separate contract from ordinary merchandising feedback.

### Customer support and service recovery

A future service workflow could correlate orders, shipments, returns, delays, and contacts to prioritize service
cases and recommend remedies. It should preserve event chronology and expose missing evidence. Sending messages, issuing
refunds, and changing orders remain caller-owned actions.

## Platform and operational boundaries

### Real-time recommendation and planning jobs

Store transforms can consume streaming-compatible inputs, but callers still own sources, checkpoints, triggers, sinks,
and query lifecycle. A future Store-specific job example could demonstrate a complete serving-and-feedback deployment,
but it should not make Structure own those lifecycle responsibilities. Any admitted stateful workflow needs explicit
watermark, late-data, state-retention, restart, and idempotence semantics.

### Data quality and contract monitoring

A future Store quality workflow could publish freshness, duplicate-key, referential-integrity, inventory-reconciliation,
and event-attribution diagnostics. It should use the same typed contracts as the business transforms and distinguish
rejected, quarantined, unknown, and valid facts. Quality monitoring should not silently repair source data in a way that
changes commercial meaning.

### Privacy and tenant isolation

As Store gains customer history, personalization, risk, and loyalty facts, a future privacy boundary may be needed for
retention windows, deletion requests, consent, purpose limitation, and tenant isolation. This belongs in explicit
schemas,
diagnostics, and caller-owned governance rather than undocumented filters in recommendation or analytics transforms.

## Permanent boundaries

The following remain caller-owned unless a separate product decision changes Structure's architecture:

- reading from commerce, warehouse, carrier, payment, or customer systems;
- writing orders, reservations, inventory, prices, promotions, refunds, or procurement actions;
- Spark session creation, stream sources and sinks, checkpoints, triggers, deployment, and recovery;
- payment authorization, financial settlement, tax calculation, and accounting ledgers;
- sending customer or operator communications; and
- production model hosting, artifact promotion, secret management, and access-control enforcement.

## Package-boundary directions

### Inventory as a first-class domain

Inventory is currently a first-class subpackage of fulfillment, with `schemas/fulfillment/inventory/` holding inventory
positions, inbound inventory, and lead times and `transforms/fulfillment/inventory/` holding dated projection logic.
That is the right boundary while the admitted inventory behavior exists to support fulfillment planning.

Inventory should become a sibling Store package only when it has an independent public contract and more than one
meaningful consumer. Candidates include snapshot normalization, reservation and receipt reconciliation, reusable
availability publication, inventory quality diagnostics, and tenant or warehouse inventory summaries. A future package
could then be organized as `schemas/inventory/` and `transforms/inventory/`, with fulfillment consuming its facts rather
than owning their preparation. Moving the current projection alone would rename a boundary without giving inventory an
independent purpose.

### Warehousing as a first-class domain

The current `Warehouse` schema is a fulfillment-planning reference: it identifies a tenant-scoped active facility and
provides region and priority fields for deterministic warehouse selection. It does not yet model warehouse operations,
so `schemas/fulfillment/warehouses/` is an appropriate home. A top-level warehousing package would be premature if it
only moved that relation without adding an independent contract.

Promote warehousing to sibling `schemas/warehousing/` and `transforms/warehousing/` packages when Store admits facility
capabilities or operational facts such as zones and locations, capacity, labor and processing calendars, cutoff times,
pick/pack capability, dock constraints, maintenance, or warehouse-level service metrics. Fulfillment should consume
those facts when selecting or evaluating a plan. Keep the domains distinct: warehousing describes facility capability,
inventory describes stock, fulfillment makes the service decision, and shipping records execution.

### Shipping as a first-class domain

Shipping should be modeled as observed execution, separate from fulfillment planning. Fulfillment decides how demand is
served; shipping records packages, carrier events, dispatch, and delivery. The current `Shipment` relation is only an
observed input to order publication, plan reconciliation, and service evaluation, so it does not yet justify a separate
shipping package.

When Store admits shipping-specific workflows—carrier-event normalization, package or split-shipment modeling,
delivery milestones, tracking, returns, or carrier-performance summaries—prefer a sibling `schemas/shipping/` and
`transforms/shipping/` package. Fulfillment should consume those observed shipping facts for plan-versus-actual
evaluation. Do not place shipping under fulfillment merely because shipments are used to evaluate fulfillment; that
would conflate the decision boundary with the execution boundary. Keep `orders/` focused on commercial order
publication, and let it consume shipping facts when needed.

## Admission guidance

Future Store work should be admitted in small vertical slices. Each slice should introduce one business contract and
one observable outcome, preserve planned-versus-observed distinctions, and add direct and generated execution evidence.
The implementation should follow the existing Store package structure: domain schemas under `examples/store/schemas/`,
focused transforms under `examples/store/transforms/`, short facade modules for app-facing pipelines, registrations in
`tests/helpers/example_projects.py`, independent Store differential tests, generated-output checks, documentation, and
`make build` validation.

The shared testing model under `res/` is not required for Store-specific admissions unless a future decision explicitly
promotes a Store capability into that separate model. Store example work should continue to use its own source fixtures,
independent references, generated artifacts, and parity tests.

## References

- Store application: `examples/store/Readme.md`
- Store design background: `docs/background/Store.back.md`; the Store README is the current public boundary.
- Search future backlog: `docs/dev/future/Search.future.md`
- Streaming future boundary: `docs/dev/deferred/Streaming.deferred.md`
- API admission criteria: `docs/dev/gated/ApiCatalog.gates.md`
