# Search Lexical Scoring


Scoring turns normalized free-form queries and reusable index artifacts into inspectable lexical evidence at four text
grains.


`ScoreOverlap` emits IDF-weighted same-grain overlap. `ScoreBm25` emits corpus-dependent BM25 with the fixed example
parameters. `SelectScores` normalizes BM25 in the grain's rank scope and combines it with independently configured
overlap and BM25 weights. `Scoring` composes those families and preserves raw component relations beside selected
scores.

Scores carry an experiment identity and `scored_at`. Query terms are expanded from free-form text using the same rules
as Indexing. Missing vocabulary and zero maxima have defined zero behavior.


`OfflineScoring` covers the configured popular queries and the preceding seven-day query population. `OnlineScoring`
fills missing or stale groups from reusable indexes. Both paths use the same score schemas and freshness policy.

## Design

BM25-only selection was rejected for the general score family because overlap is useful, inspectable evidence at every
grain. Combining raw BM25 and overlap without per-grain normalization was rejected because their scales differ. A
single global score was rejected because each grain has a different rank scope.


Scores are deterministic, grain-isolated, policy-effective, timestamped, and equivalent between offline/generated and
online execution.

The formulas are applied independently at each text grain. For query term set `Q`, target vocabulary `T(x)`,
grain-specific document frequency `df_g(t)`, and target count `N_g`:

```text
idf_g(t) = log(1 + (N_g - df_g(t) + 0.5) / (df_g(t) + 0.5))
overlap_g(q, x) = sum(idf_g(t) for t in Q ∩ T(x)) / sum(idf_g(t) for t in Q)
```

BM25 uses `k1 = 1.2` and `b = 0.75`, then is normalized within the applicable rank partition before the caller's
grain-specific BM25 and overlap weights are applied. A zero denominator or zero partition maximum produces zero,
not a missing or cross-grain score. These formulas and constants are example defaults. Applications may version their
own weights and freshness policy, but the score family, grain, partition, and source snapshot must remain explicit so
results can be reproduced.


| Family | Input grain | Rank scope |
|---|---|---|
| Lexical | Query/document | Candidate document set. |
| Feedback | Query/document/impression | Query and eligible time window. |
| Cohort | Query/document/cohort | Context-resolved candidate set. |
| Learned | Query/document/features | Declared model and feature snapshot. |

Scores carry family, policy/model version, query, candidate, and source snapshot identity. Normalization is
performed within the declared query or cohort partition; global min/max must not leak across requests. Offline
and online execution share the relation contract, even when providers differ.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Evidence | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Normalization | Alternatives in choices above | Declared request partition | Keeps lineage explicit |
| Coverage | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Freshness | Alternatives in choices above | Declared boundary | Keeps lineage explicit |

Failures must include candidate key, score family, version, and partition. Evidence should prove deterministic
ties, policy-effective selection, offline/online parity, and baseline behavior when optional scores are absent.


The corresponding implementation boundary is named by this document under `examples/search/transforms/scoring/`.
Its typed input/output definitions live under `examples/search/schemas/scoring/`. The transform describes composition
and lifecycle; the schemas define identity, grain, nullability, and output keys. The lexical rationale is part of this
contract: overlap explains shared rare terms, BM25 rewards useful term frequency without allowing long documents to
dominate, and per-grain normalization keeps scores comparable only within the partition where they were produced.
