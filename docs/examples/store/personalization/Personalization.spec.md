# Store Personalization


Personalization adds explicit customer and session preferences to the recommendation pipeline without making identity
resolution or personal history implicit.


Known shoppers are keyed by tenant and customer identity; anonymous sessions use a session identity. Product features,
customer category preferences, and session or order history become reusable relations. Empty or missing personal inputs
preserve the behavior-based recommender fallback.

The personal score is joined to a request and product only when keys match. It remains a visible component of the final
recommendation evidence and does not override hard suppression, product eligibility, or tenant isolation.

## Design

Personalization is an explicit branch rather than a hidden feature of `Recommender`, so callers can test and govern it
separately. Anonymous sessions are not silently merged into known customers. A model provider, identity graph, and
privacy/consent policy remain caller-owned future boundaries.


Customer, session, request, product, and tenant keys remain distinct; missing history has a documented fallback; and
personalized and baseline runs can be compared with the same recommendation contract.


| Concern | Contract |
|---|---|
| Identity | Customer, session, request, product, and tenant keys are never interchangeable. |
| Features | History, affinity, and consent fields carry source snapshot and freshness. |
| Join | Personalization joins only on declared tenant/request/customer scope. |
| Fallback | Missing or insufficient history selects the same baseline contract as an unpersonalized run. |
| Privacy | Consent/eligibility policy is an input boundary; the transform does not infer identity. |

Known customers and anonymous sessions are separate populations. A personalization branch may change scores or
candidate order, but it must preserve product eligibility and recommendation output identity. A provider may be
injected later, provided it returns declared features and provenance rather than hidden mutable state.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Branch | Alternatives in choices above | Explicit branch | Keeps lineage explicit |
| Anonymous identity | Alternatives in choices above | Session-scoped key | Keeps lineage explicit |
| Model provider | Alternatives in choices above | Caller provider seam | Keeps lineage explicit |

Failures must name tenant, identity type, request, feature, and consent state. Evidence should compare known,
anonymous, no-history, stale-history, and consent-restricted cases using one recommendation contract.


The corresponding implementation boundary is named by this document under `examples/store/transforms/`.
Its typed input/output definitions live under `examples/store/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
