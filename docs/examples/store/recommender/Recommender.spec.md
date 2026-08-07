# Store Recommender


The recommender boundary turns tenant-scoped product facts, shopper context, policy, and observed signals into ranked
recommendation candidates.


Candidate sources may include category retrieval, session interest, popularity, promotion, inventory, suppression, and
personalization. Hard eligibility and suppression rules are applied before final ranking. Score components remain
visible:
base eligibility, promotion, policy boost, suppression penalty, inventory, personalization, and feedback.

Ranking is deterministic: higher final score, lower suppression penalty, higher inventory boost, then product ID, with
the complete request and tenant identity in the partition. Diversification is applied after ranking and enforces
deterministic taxonomy-branch caps.

The deterministic ordering can be read as the rank key
`(-final_score, suppression_penalty, -inventory_boost, product_id)` within the tenant/request partition. The signs
express descending score and inventory preference while retaining stable product-ID ties. Suppression is applied as
eligibility evidence before this ordering, so a high score cannot resurrect a hard-suppressed product.

The flow is deliberately inspectable: signals and personalization feed candidate generation and ranking, then
diversification and publication produce the caller-facing relation. No stage reserves stock or sends a customer
message. A caller can therefore compare the transparent baseline with a replacement ranker using the same candidate,
tenant, and policy identities.

## Design

A transparent policy baseline was chosen over a hidden learned ranker. Candidate generation is separate from ranking so
semantic or learned retrieval can be added without changing score meaning. Popularity is evidence, not an eligibility
override. Diversification is last so it can explain which already-ranked items were limited.

The source shape is compact:

```python
candidates = BuildRecommendationCandidates(...).candidates
ranked = RankRecommendationCandidates(candidates=candidates, ...).ranked_candidates
published = SelectRecommendedProducts(ranked_candidates=ranked)
```

This is a flow sketch rather than a complete invocation; the schema references below define the identities that each
stage must preserve.


Hard-suppressed products never return, every recommendation has traceable source and score components, cold-start
requests retain a valid fallback, and repeated runs produce the same ranks for the same snapshot.


| Concern | Contract |
|---|---|
| Candidate admission | Only eligible catalog products and allowed source policies can enter the pool. |
| Score components | Popularity, affinity, taxonomy, and policy contributions remain inspectable. |
| Partition | Ranking is scoped by tenant, request/session, channel, and snapshot as declared. |
| Rank | Ties use stable product identity; physical row order has no meaning. |
| Diversification | Diversity is applied after ranking with an explicit cap and reason. |
| Cold start | No history selects a valid baseline, not an empty or random result. |

Generation, scoring, and diversification are separate conceptual boundaries even when composed in one example.
Hard suppression is an eligibility rule and cannot be undone by a high score. A recommendation carries enough
lineage to explain source, policy, score components, and the snapshot used for the decision.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Ranker | Alternatives in choices above | Transparent policy baseline | Keeps scoring auditable |
| Candidate source | Alternatives in choices above | Separate candidate seam | Allows retrieval replacement |
| Diversification | Alternatives in choices above | Post-rank policy | Keeps caps explainable |

Failures must include tenant, request, candidate, policy, and suppression reason. Evidence should cover hard
suppression, empty candidates, ties, cold start, duplicate product identity, and repeated snapshots.


The corresponding implementation boundary is named by this document under `examples/store/transforms/`.
Its typed input/output definitions live under `examples/store/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
