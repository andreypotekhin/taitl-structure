# Search Cohort Band Resolution


`ResolveCohortBands` maps caller-owned user profiles and cohort predicates to reusable ranking contexts. It lets
feedback and evaluation use the same context for users with the same ordered matching bands.


A user may match multiple cohort predicates. Matching applies every constrained dimension; values within one list are
alternatives. Age ranges are half-open. The resolver publishes matching memberships, a deterministic ordered `UserBand`,
and a fallback chain ending at global context. Anonymous or unmatched users use global context.

Fallback weakens the least-important matching band first through its declared parent. It does not blend sibling bands or
choose one arbitrary cohort. Missing parents, cycles, and ambiguous hierarchy identity are configuration failures.


Context resolution is upstream of feedback and search ranking. A context is a reusable relation key, not a request
attribute and not a permission decision. The resolver's recursive hierarchy expansion is a narrow raw Spark boundary;
ordinary matching and downstream ranking remain typed.

## Design

One cohort per request was rejected because a user can match independent dimensions. Score blending was rejected because
it obscures which evidence governed a result. A deterministic priority-tail fallback was chosen for explainability and
bounded context growth.


Equivalent ordered cohort sets produce reusable context identity; parent generalization is deterministic; invalid
hierarchies fail early; and adding contexts leaves global signals unchanged.


Diagnostics must name missing parents, cycles, duplicate cohort IDs, invalid age ranges, and contradictory priority
configuration. Evidence should include users with multiple matches, no matches, sparse parent feedback, anonymous users,
and two users reusing the same canonical context.


| Concern | Contract |
|---|---|
| Matching | User/context attributes are evaluated against explicit cohort predicates. |
| Age | Membership uses event-time or snapshot age, never an implicit wall-clock read. |
| Canonical key | A resolved band carries stable cohort, hierarchy, and effective-policy identity. |
| Fallback | No match and anonymous context resolve to the declared baseline band. |
| Global values | Prevalence/statistics are computed once per compatible snapshot. |
| Integrity | Parent links are acyclic, ranges are valid, and child precedence is explicit. |

Resolution is a context snapshot, not a mutable user profile. When several bands match, precedence is explicit
and deterministic; the output retains source identity so the selection can be explained. Sparse feedback may
affect statistics, but it cannot invent a user attribute or create a hidden cohort.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| User context | Alternatives in choices above | Caller-supplied row | Keeps lineage explicit |
| Sparse feedback | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Hierarchy | Alternatives in choices above | Validated parent tree | Keeps lineage explicit |

Diagnostics must identify the matched path, selected precedence, and fallback reason. Fixtures should cover
multiple matches, no match, anonymous context, sparse child feedback, and a cycle or missing parent.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
