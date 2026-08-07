# Search Corpus Similarity


The similarity boundary finds related documents, sections, paragraphs, and sentences by reusing the lexical index.


Each indexed target becomes a tagged same-grain query. Directed overlap and BM25 scores are reduced into reciprocal
relations with canonical pair identity, both BM25 directions, their mean, overlap, and a deterministic rank. At most ten
neighbors are retained per source target and grain. Self-pairs are excluded.

An optional maximum document-frequency ratio prunes common terms. Similarity does not silently apply business filters.

## Design

Query creation, shared scoring, reciprocal reduction, and presentation are separate boundaries so callers can persist
or inspect intermediate evidence. A separate embedding system was deferred; if added, lexical and vector candidate
lanes must remain visible and combine by Reciprocal Rank Fusion.

The symmetric pair is a presentation convenience, not a claim that BM25 itself is symmetric or calibrated.


Pair identity is canonical and reversible, grains do not mix, top-ten bounds hold per source, and directed evidence is
preserved for inspection and evaluation.


| Concern | Contract |
|---|---|
| Pair identity | Canonical source/candidate keys are ordered, reversible, and grain-qualified. |
| Evidence | Directed score, shared terms, and source snapshot remain available. |
| Grain | Document, section, paragraph, and sentence pair relations do not mix. |
| Bound | Top-k is applied independently per source identity. |
| Reduction | A symmetric presentation view is derived only after directed evidence exists. |

Pair generation is bounded and snapshot-aligned. A pair's identity is not a row number, and physical order
cannot determine whether it is retained. When duplicate evidence rows exist, reduction follows a declared rule;
the transform does not silently choose the first row.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Pair shape | Alternatives in choices above | Ordered pair | Keeps lineage explicit |
| Candidate control | Alternatives in choices above | Per-source top-k | Keeps lineage explicit |
| Semantic extension | Alternatives in choices above | Directed boundary | Keeps lineage explicit |

Failure evidence must identify source, candidate, grain, reduction rule, and snapshot. Fixtures should include
duplicate evidence, self-pairs, cross-grain attempts, ties, and a source with fewer than k candidates.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
