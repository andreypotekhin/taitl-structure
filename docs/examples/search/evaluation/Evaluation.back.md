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

## How it works

Both facets share a UTC-aligned daily evaluation batch, but their evidence relations remain separate. Clicks were not
chosen as relevance labels because they depend on position, interface, exposure policy, and intent. Experiment,
label, and user-band selectors compose around the base facets rather than changing metric semantics.


Observed satisfaction and judged relevance remain separate facets rather than one blended quality metric. Counterfactual
evaluation, interleaving, and session reformulation are deferred until their required exposure, experiment, and session
facts exist. A conforming implementation preserves zero-result preservation, explicit judgment coverage, stable cutoffs,
and clear naming of observed versus judged metrics.


Invalid cutoffs, duplicate judgment identity, missing batch window, inconsistent request/impression lineage, and
ambiguous experiment identity must fail early. Evidence should include no-result requests, unjudged results, all four
grades, short lists, long clicks, and separate ranking versions.


| Facet | Input grain | Output | Missing-data rule |
|---|---|---|---|
| Ranking quality | Query/result list | Ranking metric | Empty sets stay visible. |
| Judgments | Query/document judgment | Grade-aware aggregate | Unjudged differs from grade zero. |
| Engagement | Impression/click attribution | Engagement facet | Exposure stays separate. |
| Experiment | Query/variant | Variant comparison | Missing or ambiguous variant is a validation error. |

Evaluation is batch-oriented and read-only. It calculates from a bounded request window, retains query and
ranking-version identity, and makes denominator changes visible. A metric with no eligible observations has a
declared empty result rather than a fabricated zero. Metrics are descriptive unless a separate causal design is
provided.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Quality basis | Clicks as labels; judgments only; separate | Separate facets | Behavior stays separate. |
| Unknown result | Nonrelevant; drop; unjudged | Unjudged | Unknown is not zero. |
| Slicing | Hidden filters; global report; selectors | Explicit selectors | Selectors do not change metrics. |


Failures should name the request window, cutoff, experiment, and source snapshot. Examples should include
no results, no judgments, all grade values, duplicate judgment identity, and a ranking-version change.
