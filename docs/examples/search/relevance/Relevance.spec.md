# Search Relevance Signals


Relevance builds transparent, batch ranking evidence from persisted daily exposure and engagement facts. It supplies
feedback to document reranking but does not redefine relevance or evaluate corpus quality.


Signals are emitted at query-document and document-popularity grains. They retain exposure, raw click, binary
clicked-impression, dwell, long-click, CTR, propensity-weighted, and normalized components. The default policy applies
recency decay, capped dwell credit, self-normalized inverse propensity, a 70/30 dwell/CTR blend, and a minimum exposure
threshold for CTR. Document reranking applies its separate query/global feedback mix.

Band-scoped signals use the exact context and its ordered fallback chain. Sparse contexts fall through to the first
qualifying context and ultimately global; siblings are not blended.

## Design

CTR counts impressions, not click events, while raw clicks remain observable. A daily streaming snapshot was chosen
over exact whole-history streaming normalization because decay and normalization require bounded batch input. Search
does
not infer propensity, personalize implicitly, or learn weights in this boundary.


Unclicked exposures remain denominators, repeated clicks do not inflate CTR beyond one per impression, sparse feedback
does not remove lexical candidates, and global values remain stable when bands are introduced.


| Signal | Grain/key | Role |
|---|---|---|
| Lexical | Query/document or query/text target | Candidate relevance baseline. |
| Feedback | Query/document/impression | Observed engagement evidence with denominator. |
| Cohort | Query/document/cohort band | Contextual adjustment with fallback identity. |
| Global | Corpus/query snapshot | Smoothing and sparse-data reference. |

Signals combine only after their grains and snapshot identities are compatible. CTR is bounded and uses
impressions as its denominator; repeated clicks are bounded by impression policy. Decay, inverse propensity, and
smoothing are policy values, not hidden constants. Sparse evidence falls back toward global or lexical behavior.

The main ratios make the evidence boundaries explicit:

```text
CTR(q, d) = clicked_impressions(q, d) / impressions(q, d)
IPS_CTR = sum(decay_i * clicked_i / propensity_i)
          / sum(decay_i / propensity_i)
feedback_signal = 0.70 * normalized_dwell + 0.30 * IPS_CTR
```

CTR becomes eligible only after the declared minimum exposure threshold, and query-specific and global signals are
combined with the example's 80/20 policy before document reranking. These defaults are versioned policy choices, not
claims that clicks are calibrated relevance probabilities.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Evidence | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| CTR | Raw ratio; capped binary ratio; event count | Capped binary ratio | Replays cannot dominate a result. |
| Sparse data | Alternatives in choices above | Smoothed fallback | Keeps lineage explicit |
| Weighting | Alternatives in choices above | Policy artifact boundary | Keeps lineage explicit |

Failures must identify incompatible grains, missing denominators, invalid policy timestamps, and stale cohort
context. Evidence should compare no-feedback, sparse-feedback, repeated-click, and new-cohort cases.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
