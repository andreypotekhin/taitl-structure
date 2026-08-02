# Search app future

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
- embeddings, vector search, answer-model invocation, and cross-document answer-context assembly;
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
- an embedding or vector-search implementation: implement vector index
- streaming-job framework: ensure streaming queries when searching
- adaptive passage chunking, configurable context radii, session reformulation metrics
