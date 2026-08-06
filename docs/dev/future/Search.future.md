# Search app future

## Scheduled v10 slice

The focused vector-index and Reciprocal Rank Fusion work is scheduled for v10 Sprints 49–54 under
[`P08052602.Search-vector-index-and-rrf.plan.md`](../planning/P08052602.Search-vector-index-and-rrf.plan.md). The adopted
slice is limited to caller-supplied embeddings, typed exact vector artifacts, document and paragraph similarity, and
hybrid document-search candidate fusion.

The deferred inventories below remain intentionally preserved. The v10 plan does not adopt model invocation, external
approximate-nearest-neighbor services, answer-model invocation, cross-document answer-context assembly, adaptive
chunking, streaming vector-index maintenance, or the other future work listed here. The broader “embeddings, vector
search” entries remain as umbrella future items until the scheduled slice is implemented and its remaining boundaries
are split into their own admitted work.

## Deferred Work - Search.md
Include Deferred Work from Search.md

Ref: [Search.md](/docs/dev/design/Search.md)
Quote:
````
Deferred Work
- adaptive passage chunking and caller-configurable context radii;
- embeddings, vector search, answer-model invocation, and cross-document answer-context assembly;
- ERR, which the existing four-grade judgment contract can support later without migration;
- Accuracy@N, because the proposed definition duplicates Precision@N;
- experiment comparison and interleaving, which require experiment-arm and displayed-result-ownership facts;
- counterfactual policy evaluation, which requires logged randomized selection probabilities; and
- session reformulation metrics, which require a stable session identifier.
- propensity calibration, clipping, drift monitoring, and experiment assignment validation, which are serving-system
  responsibilities rather than transformations over already-logged facts;
- impression-level fraud, bot, and accidental-click classification, which requires identity and trust contracts; and
- learned feedback weights, personalized signals, and feedback-loop guardrails, which require held-out evaluation,
  feature governance, and deployment controls beyond the fixed transparent example policy.
````

To adopt from above:
- adaptive passage chunking and caller-configurable context radii;
- partial v10 adoption through `P08052602.Search-vector-index-and-rrf.plan.md`: caller-supplied exact vector index and
  RRF retrieval for documents and paragraphs; model invocation, external ANN search, answer-model invocation, and
  cross-document answer-context assembly remain deferred;
- ERR, which the existing four-grade judgment contract can support later without migration;
- counterfactual policy evaluation, which requires logged randomized selection probabilities; and
- session reformulation metrics, which require a stable session identifier.
- propensity calibration, clipping, drift monitoring, and experiment assignment validation, which are serving-system
  responsibilities rather than transformations over already-logged facts;
- learned feedback weights, personalized signals, and feedback-loop guardrails, which require held-out evaluation,
  feature governance, and deployment controls beyond the fixed transparent example policy.

## Deferred Work - Search.back.md
Include Deferred Work from Search.back.md

Ref: [Search.back.md](/docs/background/Search.back.md)
Quote:
````
Boundaries and Deferred Work
The Search example is intentionally not a crawler, a document store, an answer service, a prompt assembler, an
embedding or vector-search implementation, or a streaming-job framework. It also does not prescribe adaptive passage
chunking, configurable context radii, experiment comparison, counterfactual policy evaluation, or session
reformulation metrics.
````

To adopt from above:
- crawler pipeline: starting with HTML/MD, convert to Search-format docs
- answer service: implement question-answer search
- an embedding or vector-search implementation: the bounded exact vector-index/RRF slice is scheduled by
  `P08052602.Search-vector-index-and-rrf.plan.md`; external ANN/vector services and the remaining provider boundary
  stay deferred
- streaming-job framework: ensure streaming queries when searching
- adaptive passage chunking, configurable context radii, session reformulation metrics
