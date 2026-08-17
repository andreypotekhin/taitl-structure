# Search Lexical Scoring


Scoring turns normalized free-form queries and reusable index artifacts into inspectable lexical evidence at four text
grains.


`ScoreOverlap` emits IDF-weighted same-grain overlap. `ScoreBm25` emits corpus-dependent BM25 with the fixed example
parameters. `SelectScores` normalizes BM25 in the grain's target scope and combines it with independently configured
overlap and BM25 weights. `ScoreVectors` adds document and paragraph cosine evidence under a separate vector policy.
`Scoring` composes the lexical and vector families and preserves raw component relations beside selected scores.

Scores carry an experiment identity and `scored_at`. Query terms are expanded from free-form text using the same rules
as Indexing. Missing vocabulary and zero maxima have defined zero behavior. Vector rows also carry model, dimension,
revision, and experiment compatibility identity.


`OfflineScoring` covers the configured popular queries and the preceding seven-day query population. `OnlineScoring`
fills missing or stale lexical and vector groups from reusable indexes within the prefilter target scope. Both paths
use the same score schemas and freshness policy.

## How it works

The general score family combines BM25 with overlap because overlap is useful, inspectable evidence at every grain.
Each component is normalized within its grain because their scales differ, and each grain keeps its own rank scope
instead of sharing one global score.


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
| Lexical | Query/document | Prefilter target set. |
| Vector | Query/document or paragraph | Target-scoped vector index. |
| Feedback | Query/document/impression | Query and eligible time window. |
| Cohort | Query/document/cohort | Context-resolved candidate set. |
| Learned | Query/document/features | Declared model and feature snapshot. |

Scores carry family, policy/model version, query, candidate, and source snapshot identity. Normalization is
performed within the declared query or cohort partition; global min/max must not leak across requests. Offline
and online execution share the relation contract, even when providers differ.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Evidence | BM25 only; opaque blend; separate | Separate families | Families stay inspectable. |
| Normalization | Global max; raw combination; grain partition | Request partition | Partitions contain scale. |
| Coverage | All offline; popular; popular + recent | Popular + recent | Coverage stays bounded. |
| Freshness | Latest row; machine clock; effective policy | Effective policy | Policy and snapshot match. |


Failures must include candidate key, score family, version, and partition. Useful examples prove deterministic
ties, policy-effective selection, offline/online parity, and baseline behavior when optional scores are absent.
