# Search Repeat-Query Dependency Map

Status: source-level inventory plus cached-fixture handwritten parity; caller revision semantics and repeat-query latency baseline remain open

This map is the first M1 artifact for
[I09282601](../I09282601.Search-repeat-query-latency.issue.md). It describes
the current `SearchDocuments` inputs and the stages that consume them. It is
not evidence that an input is immutable across invocations: an ordinary
DataFrame input can change between plans if its source or caller-managed
snapshot changes. A prepared run must bind explicit caller-owned corpus and
policy revisions rather than infer immutability from object identity or the
`streaming` declaration.

## Input roles

| Role in current component | Inputs | Repeat-query implication |
| --- | --- | --- |
| New request facts | `queries`, `requests` | Always vary per request. Request IDs, query text/time, user, experiment, and scope determine joins and output identity. |
| Live candidate additions | `streamed_documents`, `streamed_document_scores`, optional `document_filter_targets` | May change during a long-running request stream. Filtering, score freshness, and candidate scope must remain consistent with the selected snapshot/revision contract. |
| Corpus/index snapshot | `documents`, `document_terms`, `document_summary`, `document_vector_index` | Candidate retrieval and lexical/vector scoring depend on these. A caller may reuse them only while the corresponding corpus/index revision is fixed. |
| Cached or precomputed score inputs | `document_scores`, `document_vector_scores`, `document_overlap_scores`, `document_filter_scores`, `document_vector_embeddings` | Online scoring/vectorization uses cache and freshness logic. Reuse depends on content, model, experiment, and freshness revisions; a fixed corpus alone does not prove these rows remain authoritative. |
| Filtering and scoring policy | `score_policy`, `gap_policy`, `vector_policy`, `inference_policy` | Changes candidate admission, fallback computation, vector eligibility, and fusion limits. Must be revision-bound independently from corpus data. |
| User/scope feedback | `band_memberships`, `query_document_signals`, `document_popularity`, `band_fallbacks`, `policy` | Changes grouping, personalization, feedback lookup, fallback selection, or final score. May be user-, band-, experiment-, or time-sensitive and must not be captured across requests unless its effective revision is fixed. |

The first column reflects how the existing component uses inputs, not an
immutability guarantee. The current declaration marks request and selected
candidate inputs as streaming-capable, but `SearchDocuments(streaming=True)`
still compiles as batch-only on Spark 3.5 and 4.0. Stream capability must not
be read as proof that the full graph can execute incrementally.

## Stage dependency and semantic hazards

`OnlineFiltering` chooses targets from query/request facts, filter inputs,
terms, and score policy. `OnlineVectorization` uses request queries and
filtered targets together with embeddings, document/index data, and inference
and vector policies. `OnlineScoring` merges streamed and cached lexical,
vector, and overlap score paths, consulting terms, summary data, policies, and
freshness rules. These are request-dependent even if some source tables can
be prepared once.

`RetrieveDocuments` joins the scores to documents, requests, query rows, band
memberships, and filter targets. `FuseDocuments` separately ranks lexical
and vector candidates, checks uniqueness, limits vector candidates using
`vector_policy.maximum_candidates`, merges the branches, computes reciprocal
rank fusion, and ranks the combined results. `RerankDocuments` uses
query-document signals, popularity, band fallbacks, and relevance policy;
normalization uses the full candidate maximum within each query/band/experiment
scope before it publishes the first 100 rows from at most 1,000 rerank
candidates.

Therefore, retaining only the apparent final top K before score refresh,
uniqueness checks, vector fusion, or normalization is not known to be
equivalent. New score contributions can change ranking; strict duplicate
validation currently covers rows outside the returned top K. Any streaming
prototype must compare final rows and failure behavior with batch Search over
the same explicitly finite accepted input set.

## M1 work still required

`tests/integration/pyspark/search/support/handwritten_search.py` now provides
an independently expressed PySpark reference for the existing cached lexical
reranking fixture. The ordinary parity test checks online Structure, generated
Structure, and the handwritten path on Spark 3.5/4.0 and Spark Connect 4.0. An
opt-in sequence additionally checks exact rows and schemas for four controls:
Structure online/generated, handwritten batch, and prepared handwritten
batch. The prepared fixture caches fixed document/popularity inputs and hoists
fixed policy scalars; it does not implement full online scoring, vector
retrieval, all validation/failure behavior, or a reusable production handle.
It is not the full batch-equivalent Search reference required for score misses.

Spark Connect workers do not inherit the test client's local Python import
path. The integration fixture now distributes the repository's `examples`
and `structure` modules to Connect executors with Spark's Python artifact
support before running worker UDFs. A direct executor import probe passes on
Connect 3.5 and 4.0, and the Search reranking case passes on Connect 4.0. The
initial rerun also found that the local Connect image had a stale baked
runner; rebuilding it activated the existing Spark 4.0 checkpoint-directory
setting needed by Search's eager checkpoints.

The new opt-in runner completed one Spark 3.5 cohort with one distinct request
per mode. It recorded 42.91/46.08 seconds for Structure online/generated
construction, compared with 0.74 seconds for handwritten batch and 0.38 seconds
for prepared handwritten construction after 0.50 seconds of preparation.
Collection took 0.43–0.67 seconds. These are one-sample cached-score results,
not medians and not evidence about score-cache misses. Reproduce from
`scripts/benchmark_search_repeat_queries.py`; raw logs, request samples, and
dirty source hashes are under `docs/dev/issues/I09282601/pyspark35-smoke/`.
The acceptance baseline still requires independent cohorts, 20 distinct
requests, score-miss coverage, and supported Spark 4/Connect variants. The
runner directly collects DataFrames so the identity-based helper cache in
`tests/integration/pyspark/support/rows.py` cannot turn repeats into Python
cache hits.
