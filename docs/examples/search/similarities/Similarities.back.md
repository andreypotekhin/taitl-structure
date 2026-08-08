# Search Corpus Similarity


The similarity boundary finds related documents, sections, paragraphs, and sentences by reusing the lexical index.


Each indexed target becomes a tagged same-grain query. Directed overlap and BM25 scores are reduced into reciprocal
relations with canonical pair identity, both BM25 directions, their mean, overlap, and a deterministic rank. At most ten
neighbors are retained per source target and grain. Self-pairs are excluded.

An optional maximum document-frequency ratio prunes common terms. Similarity does not silently apply business filters.

## How it works

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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Pair shape | Unordered; one directed row; canonical | Ordered pair | Direction stays meaningful. |
| Candidate control | Global top-k; no cap; per-source top-k | Per-source top-k | Budget is per source. |
| Semantic extension | Replace; hidden vector; directed | Directed boundary | Future semantics stay explicit. |


Failures should identify source, candidate, grain, reduction rule, and snapshot. Examples should include
duplicate evidence, self-pairs, cross-grain attempts, ties, and a source with fewer than k candidates.
