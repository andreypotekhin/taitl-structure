# Store Evaluation


Store evaluation measures observed recommendation behavior and fulfillment service outcomes without turning either into
an unqualified business-success claim.


Recommendation evaluation retains zero-result requests and summarizes request, impression, click, purchase, and
exposure-adjusted behavior by strategy and policy version. Click and purchase attribution use explicit impression
windows
and identity keys.

Fulfillment evaluation matches observed shipment facts to planned lines by tenant, order ID, and line number. It reports
on-time, in-full, lateness, and target-attainment outcomes while preserving missing or partial shipment evidence.

Recommendation behavior uses a 24-hour impression-to-click attribution window and a 30-day impression-to-purchase
window. Fulfillment metrics use observed shipment dates when available; a planned date is never substituted silently.
These windows and date rules keep evaluation descriptive and make its denominators reproducible.

## Design

Behavior metrics are descriptive because stable variant assignment alone does not provide randomization or selection
probabilities. Fulfillment service metrics use actual shipment dates, not planned dates as substitutes. Causal
experiments, confidence intervals, returns, and carrier performance require additional facts and are deferred.


No-result and unshipped lines remain visible, duplicate product lines are not collapsed, attribution windows are
explicit,
and evaluation labels do not imply causality or financial accounting truth.


| Facet | Input | Output | Missing behavior |
|---|---|---|---|
| Recommendation | Request/product exposure | Declared boundary | Keeps lineage explicit |
| Fulfillment | Order line and shipment | Declared boundary | Keeps lineage explicit |
| Attribution | Event plus serving lineage | Windowed descriptive label | Window and source identity are retained. |
| Commercial | Amount/quantity facts | Descriptive measures | Accounting authority remains external. |

Evaluation uses declared populations and business dates. Duplicate product lines remain separate when their line
identity differs. Planned dates cannot substitute for observed shipment dates without an explicit policy marker.
An evaluation label describes observed behavior; it does not establish causality or authorize a financial action.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Recommendation quality | Alternatives in choices above | Separate facets | Keeps lineage explicit |
| Shipment date | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Causal claim | Alternatives in choices above | Descriptive label | Keeps lineage explicit |

Failure evidence must include population, line/product identity, window, business date, and source snapshot.
Fixtures should include no result, no shipment, duplicate lines, late events, and zero-denominator facets.


The corresponding implementation boundary is named by this document under `examples/store/transforms/`.
Its typed input/output definitions live under `examples/store/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
