# Search Example Backgrounds

These backgrounds describe the Search example one public boundary at a time. Search is a caller-owned typed
transformation system: lexical scores, interaction signals, relevance judgments, and evaluation summaries are
different evidence families with different grains and timestamps. The example keeps those families separate so a
click is not silently treated as a relevance label, a corpus snapshot is not confused with serving state, and an
offline score is not assumed to be a calibrated probability.

The directory layout mirrors `examples/search/transforms/` at its top level. Read a topic from its opening explanation
through the continuous design narrative and decision tables. Each background accumulates the topic's contract, formulas,
alternatives, resolved choices, and failure evidence so a reader does not need to reconstruct the decision from a
separate specification or discussion. Annotated source remains the place for line-by-line code orientation.

## Boundaries

- All — complete offline artifact composition.
- Chunking — document hierarchy and sentence boundary.
- Clicks and Impressions — serving-event aggregation.
- ResolveCohortBands — reusable user contexts and fallback.
- Evaluation — judged quality and observed behavior.
- Experiments — named score and reranking variants.
- Features — optional training features.
- Filtering — document candidate prefiltering.
- Indexing — reusable lexical artifacts.
- Labeling — query labels and intent slices.
- Relevance — exposure-aware feedback snapshots.
- Scoring — lexical score families and offline coverage.
- Searching, SearchPassages, SearchSentences, and SearchSimilarity — presentation boundaries.
- Similarities — same-grain corpus similarity.
- Statistics, CorpusText, and ProfileDocuments — descriptive corpus facts.
- Training — optional offline ranking artifacts.

## Shared rules

Every boundary consumes and emits typed relations, keeps caller ownership of persistence and orchestration, preserves
deterministic rank tie-breakers, and must remain equivalent in online and generated-code execution. A relation's key,
grain, effective snapshot, null policy, and failure evidence are part of its contract even when the implementation
provider changes.

The current Search implementation is lexical. Vector retrieval and Reciprocal Rank Fusion remain architecture
alternatives rather than available behavior because their additional index state and score calibration would change
the evidence contract. SearchDocuments remains batch-only until bounded streaming state and append-only finalization
are proven.

## Architecture map

| Plane | Boundaries | Primary artifact | State owner |
|---|---|---|---|
| Preparation | Chunking, Indexing, Statistics, Features | Snapshot-aligned corpus facts | Caller-owned batch snapshot |
| Evidence | Clicks, Impressions, Relevance, Labeling | Exposure and judgment relations | Caller-owned event history |
| Ranking | Filtering, Scoring, Training, Experiments | Versioned score families | Query execution or run |
| Presentation | Searching and SearchSimilarity families | Bounded result relations | Query execution boundary |
| Evaluation | Evaluation, Experiments, All | Comparative/descriptive reports | Offline run identity |

## Shared architectural decisions

| Decision point | Alternatives | Chosen result | Why |
|---|---|---|---|
| Retrieval | Lexical/vector/hybrid | Lexical baseline | Inspectable term evidence; dependency-light example. |
| Extension | Whole pipeline/hidden hooks/seams | Named seams | Add rankers without changing identity/grain contracts. |
| State | Managed stores/implicit caches/caller snapshots | Caller snapshots | Freshness and replay remain visible. |
| Publication | Effects/mutable objects/relations | Return relations | Callers choose persistence and serving. |

The contract is relation-first: key, grain, effective snapshot, and failure policy are more stable
than a particular tokenizer, ranker, or storage engine. The implementation boundaries named by this index live under
`examples/search/transforms/`; their field and identity definitions live under `examples/search/schemas/`. Those
directories are source orientation, while the prose here defines the behavior that must survive an implementation
change. Each topic background adds the boundary-specific invariants and evidence needed to implement or replace
that boundary safely.
