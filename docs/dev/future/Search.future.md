# Search app future

## Scheduled v10 slice

The focused vector-index and Reciprocal Rank Fusion work is implemented under the v10 proving slice in
`P08052602.Search-vector-index-and-rrf.plan.md`, with the architecture follow-up recorded in
`P08102602.Similarity-search-hybrid-and-ann-backends.plan.md`. The implemented slice accepts caller-supplied
embeddings, provides typed exact reference artifacts, supports document and paragraph hybrid similarity, and exposes a
provider-neutral ranked-candidate boundary for caller-owned ANN implementations.

The deferred inventories below remain intentionally preserved. The v10 slice does not adopt model invocation, operate
an external approximate-nearest-neighbor service, provide answer-model invocation, assemble cross-document answer
context, implement adaptive chunking, or maintain a streaming vector index. ANN services remain caller- or plugin-owned;
Structure consumes their typed ranked candidates at the same boundary as the exact reference producer.

## Deferred Work - Search.md
Include Deferred Work from Search.md

Reference concept: Search design and its evidence boundaries.
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
- partial v10 adoption through `P08052602.Search-vector-index-and-rrf.plan.md` and
  `P08102602.Similarity-search-hybrid-and-ann-backends.plan.md`: caller-supplied exact vector index, provider-neutral
  candidates, and RRF retrieval for documents and paragraphs; model invocation, hosted ANN operation, answer-model
  invocation, and cross-document answer-context assembly remain deferred;
- ERR, which the existing four-grade judgment contract can support later without migration;
- counterfactual policy evaluation, which requires logged randomized selection probabilities; and
- session reformulation metrics, which require a stable session identifier.
- propensity calibration, clipping, drift monitoring, and experiment assignment validation, which are serving-system
  responsibilities rather than transformations over already-logged facts;
- learned feedback weights, personalized signals, and feedback-loop guardrails, which require held-out evaluation,
  feature governance, and deployment controls beyond the fixed transparent example policy.

## Deferred Work - Search.back.md
Include Deferred Work from Search.back.md

Reference concept: Search background and its deferred-work boundaries.
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
- an embedding or vector-search implementation: the bundled exact vector-index/RRF path is the reference producer;
  caller-owned external ANN/vector services may replace it by emitting the documented candidate relation
- streaming-job framework: ensure streaming queries when searching
- adaptive passage chunking, configurable context radii, session reformulation metrics
