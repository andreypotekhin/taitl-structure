# Store Example Application


Store demonstrates a multi-tenant retail flow that begins with product facts and recommendations, admits commercial
demand, plans fulfillment, observes shipment outcomes, and publishes operational summaries. It shows how Structure can
keep business decisions explicit while leaving external actions with the caller.


The principal funnel is:

    catalog + taxonomy
      -> recommendation candidates + shopper context
      -> served recommendation evidence
      -> commercial orders
      -> fulfillment demand and inventory plan
      -> observed shipment facts
      -> reconciliation, service evaluation, and analytics

Recommendations answer which products to show before an order exists. Fulfillment answers how an admitted order line
can be served. Shipping facts answer what actually happened. These are separate questions and remain separate packages.


Store owns typed transformations, tenant-scoped joins, transparent policy components, deterministic ranking and
allocation rules, and generated/online parity. The caller owns product and order sources, storage, stream lifecycle,
reservations, purchase orders, shipment actions, payment and tax systems, model deployment, and communications.

No Store transform sends a message, reserves inventory, issues a purchase order, authorizes payment, or creates a
shipment. Suggestions are descriptive outputs for a caller-owned decision process.

## How it works

- Keep recommendations separate from fulfillment so pre-demand merchandising is not confused with post-order service.
- Preserve tenant identity in every relation that can cross organizational boundaries.
- Treat promotions, feedback, inventory, and personalization as visible score components rather than opaque authority.
- Keep one order line distinct even when product IDs repeat; line identity is tenant, order, and line number.
- Keep planned warehouse and actual shipment warehouse separate until actual execution facts exist.
- Keep behavior evaluation descriptive; stable assignment is not a causal experiment.


Store uses caller-owned transformations rather than a hosted commerce workflow. Splitting an order line across
warehouses, automatic procurement, dynamic promise calendars, returns, carrier workflows, learned ranking, causal
experimentation, and privacy governance remain future boundaries. Inventory, warehousing, and shipping stay nested or
caller-owned until each has an independent workflow and evidence contract.


A fixture can flow through the main stages with tenant-safe identity, stable output schemas, explicit planned-versus-
observed distinctions, and deterministic results. Omitting optional personalization, feedback, or experiments preserves
the transparent baseline behavior.


| Concern | Contract |
|---|---|
| Identity | Tenant, product, order, line, warehouse, request, and experiment keys stay distinct. |
| Planned vs observed | Forecast, demand, allocation, shipment, and reconciliation facts are separate. |
| State | Each relation names its source snapshot and effective policy/version. |
| Fallback | Optional personalization, feedback, and experiments may be absent safely. |
| Side effects | Transforms return relations; reservations, payments, shipping, and publication stay external. |
| Evaluation | Descriptive metrics retain population, date, and source identity. |

The Store composition is a typed funnel from catalog facts to commercial demand, operations, and measurement.
Joins must be tenant-safe and cardinality-aware. A relation that multiplies rows declares the multiplication
and provides a reconciliation key; consumers must not infer one order or line from physical row count.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Boundary | Hosted service; typed transforms; hidden effects | Typed transforms | Callers retain side effects. |
| Inventory | Stock; warehouse snapshot; availability | Warehouse snapshot | Allocation stays tenant-safe. |
| Order state | Mutable status; planned/observed; status only | Planned/observed facts | Plans stay separate. |
| Optional branches | Required; hidden defaults; explicit baseline | Explicit baseline | Omission is safe. |

Failure evidence must include the first invalid tenant key, join cardinality, policy version, or source snapshot.
Examples should flow one order and one recommendation through the stages, then repeat with optional branches
absent and with a deliberate cross-tenant mismatch.
