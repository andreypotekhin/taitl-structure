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

## How it works

Behavior metrics are descriptive because stable variant assignment alone does not provide randomization or selection
probabilities. Fulfillment service metrics use actual shipment dates, not planned dates as substitutes. Causal
experiments, confidence intervals, returns, and carrier performance require additional facts and are deferred.


No-result and unshipped lines remain visible, duplicate product lines are not collapsed, attribution windows are
explicit,
and evaluation labels do not imply causality or financial accounting truth.


| Facet | Input | Output | Missing behavior |
|---|---|---|---|
| Recommendation | Request/product exposure | Recommendation facet | Behavior remains separately measurable. |
| Fulfillment | Order line and shipment | Service facet | Actual shipment facts remain distinct. |
| Attribution | Event plus serving lineage | Windowed descriptive label | Window and source identity are retained. |
| Commercial | Amount/quantity facts | Descriptive measures | Accounting authority remains external. |

Evaluation uses declared populations and business dates. Duplicate product lines remain separate when their line
identity differs. Planned dates cannot substitute for observed shipment dates without an explicit policy marker.
An evaluation label describes observed behavior; it does not establish causality or authorize a financial action.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Recommendation quality | Clicks; judgments; separate facets | Separate facets | Observed behavior stays distinct. |
| Shipment date | Planned date; event date; wall clock | Observed event date | Service metrics use actual outcomes. |
| Causal claim | Causal; pooled; descriptive label | Descriptive label | Assignment alone is not causality. |

Failure evidence must include population, line/product identity, window, business date, and source snapshot.
Examples should include no result, no shipment, duplicate lines, late events, and zero-denominator facets.
