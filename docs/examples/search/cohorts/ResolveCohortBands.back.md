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

## How it works

Users may match independent cohort dimensions, but sibling bands are not blended. A deterministic priority-tail fallback
keeps the governing evidence explainable and bounds context growth.


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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| User context | One band; sibling blend; ordered | Caller-supplied row | Dimensions remain visible. |
| Sparse feedback | Drop sparse; global; fallback | Fallback chain | Sparse behavior stays visible. |
| Hierarchy | Unbounded recursion; flat bands; tree | Parent tree | Invalid trees fail early. |


Diagnostics must identify the matched path, selected precedence, and fallback reason. Examples should cover
multiple matches, no match, anonymous context, sparse child feedback, and a cycle or missing parent.
