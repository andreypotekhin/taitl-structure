# Search Evaluation


Search evaluation provides evidence for two different claims: whether returned documents match explicit relevance
judgments, and how users interacted with the list that was actually served.


`EvaluateDocumentRanking` evaluates one result run against caller-owned four-grade judgments. Grades 2 and 3 are binary
relevant for precision, judged recall, success, and reciprocal rank; every grade contributes to nDCG. Returned
unjudged documents make affected metrics unavailable rather than silently nonrelevant. Compare runs against the same
judgment pool.

`EvaluateDocSearchBehavior` evaluates requests, impressions, and clicks. It retains no-result requests and reports
served-list outcomes such as result counts, click and long-click flags, first satisfying rank, and exposure-adjusted
rates. A long click is at least ten seconds of dwell.

Neither facet is a calibrated probability of relevance: judgments describe assessor ratings, while behavior describes
what happened after exposure. Clicks remain separate from relevance labels because behavior is affected by position,
propensity, interface choice, and user intent; an assessor judgment is a different kind of evidence.

## Design

Both facets share a UTC-aligned daily evaluation batch, but their evidence relations remain separate. Clicks were not
chosen as relevance labels because they depend on position, interface, exposure policy, and intent. Experiment,
label, and user-band selectors compose around the base facets rather than changing metric semantics.


A single blended quality metric was rejected because it would make observed satisfaction look like judged relevance.
Counterfactual evaluation, interleaving, and session reformulation are deferred until their required exposure,
experiment, and session facts exist. Acceptance requires zero-result preservation, explicit judgment coverage, stable
cutoffs, and clear naming of observed versus judged metrics.


Invalid cutoffs, duplicate judgment identity, missing batch window, inconsistent request/impression lineage, and
ambiguous experiment identity must fail early. Evidence should include no-result requests, unjudged results, all four
grades, short lists, long clicks, and separate ranking versions.


| Facet | Input grain | Output | Missing-data rule |
|---|---|---|---|
| Ranking quality | Query/result list | Declared boundary | Keeps lineage explicit |
| Judgments | Query/document judgment | Grade-aware aggregate | Unjudged differs from grade zero. |
| Engagement | Impression/click attribution | Declared boundary | Keeps lineage explicit |
| Experiment | Query/variant | Variant comparison | Missing or ambiguous variant is a validation error. |

Evaluation is batch-oriented and read-only. It calculates from a bounded request window, retains query and
ranking-version identity, and makes denominator changes visible. A metric with no eligible observations has a
declared empty result rather than a fabricated zero. Metrics are descriptive unless a separate causal design is
provided.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Quality basis | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Unknown result | Alternatives in choices above | Explicit unjudged state | Keeps lineage explicit |
| Slicing | Alternatives in choices above | Declared boundary | Keeps lineage explicit |

Failure evidence must name the request window, cutoff, experiment, and source snapshot. Fixtures should include
no results, no judgments, all grade values, duplicate judgment identity, and a ranking-version change.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
