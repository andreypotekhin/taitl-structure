# Store Catalog


The catalog boundary establishes tenant-visible product facts before recommendations or orders consume them.


Catalog preparation admits active products for the tenant, excludes blocked products, and attaches descriptive promotion
evidence without turning a promotion into eligibility. Normalization supplies stable identifiers and downstream category
keys. Missing optional descriptive facts remain distinguishable from an ineligible product.

Catalog outputs are reusable inputs to recommendation serving and commercial order validation. They do not reserve
stock,
set prices, publish a campaign, or call a product system.

## Design

Catalog preparation was kept separate from recommendation ranking so eligibility cannot be accidentally changed by score
policy. Tenant-scoped joins were chosen over globally unique product assumptions. Promotions remain ranking evidence;
forcing promoted products into results was rejected.


Blocked or inactive products cannot enter eligible outputs, tenant identity is preserved across joins, normalization is
deterministic, and the same catalog can feed both recommendation and order paths.


| Concern | Contract |
|---|---|
| Eligibility | Blocked, inactive, or out-of-window products do not enter eligible output. |
| Tenant key | Product identity is tenant-qualified through every join and lookup. |
| Normalization | Names, categories, and searchable values use a deterministic policy/version. |
| Promotion | Price/promotion facts retain effective interval and source identity. |
| Stock | Availability is a snapshot fact, not a reservation or allocation action. |
| Reuse | Recommendation and order consumers share catalog identity but may apply different policies. |

Catalog preparation separates product master facts from eligibility policy and stock state. A product may exist
in the master relation while being ineligible for a channel, date, or tenant. Effective intervals are evaluated
against the declared business timestamp, not the machine clock.

The eligibility shape is `active AND tenant_visible AND NOT blocked`; promotion evidence may raise a score but cannot
override that predicate. This separation lets the same catalog output feed recommendation and fulfillment paths
without allowing either consumer to redefine eligibility. A promotion may be useful evidence for ranking, but it
cannot make an inactive or blocked product valid.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Eligibility | Alternatives in choices above | Policy-filtered view | Keeps lineage explicit |
| Promotion | Alternatives in choices above | Effective-dated facts | Keeps lineage explicit |
| Identity | Alternatives in choices above | Declared boundary | Keeps lineage explicit |

Failure evidence must include tenant, product, policy timestamp, and exclusion reason. Fixtures should cover
blocked products, inactive periods, duplicate IDs, normalization variants, and empty eligible output.


The corresponding implementation boundary is named by this document under `examples/store/transforms/`.
Its typed input/output definitions live under `examples/store/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
