# Search Passage Presentation


`SearchPassages` presents paragraph-level lexical evidence with enough local context for a caller to cite or assemble
an answer context.


The transform accepts the original documents plus paragraph and section boundaries. Each result contains the matched
paragraph, document title and URL, section heading, lexical score, and nullable
preceding and following paragraph content. Context may include only adjacent paragraphs in the same document section.

Only the matched paragraph contributes terms and rank. Adjacent matched paragraphs remain separate results, and the
caller chooses top-K, overlap removal, citation policy, and answer assembly.

## Design

Paragraphs were chosen as the initial passage grain because they preserve source context without imposing an answer
model. Cross-heading context was rejected because it can join unrelated sections. Adaptive chunking and configurable
context radii remain future choices.


Rank does not change when neighbor content is added, section boundaries are never crossed, and physical DataFrame order
is not used for paging.


| Field group | Contract |
|---|---|
| Passage identity | Document, section, paragraph, passage ordinal, query, and snapshot remain distinct. |
| Neighbor radius | Context expansion is bounded by a declared same-section radius. |
| Boundary | Neighbor lookup never crosses section or document identity. |
| Ranking | The center passage score and rank are computed independently of optional context text. |
| Paging | Page boundaries use stable rank/identity, not physical DataFrame order. |

Neighbor text is presentation context. It may enrich a result, but it cannot change the candidate identity or
ranking unless a separate policy explicitly says so. Missing neighbors produce a shorter context, not a fabricated
cross-boundary passage.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Result grain | Document; sentence; passage | Passage with parent identity | Keeps lineage explicit |
| Radius | Whole section; fixed local radius; caller text | Fixed local radius | Work and output size remain bounded. |
| Result policy | Alternatives in choices above | Display-only context | Keeps lineage explicit |

Failures must report center and neighbor identity, section boundary, radius, and snapshot. Fixtures should include
first/last passages, missing neighbors, multiple sections, and reordered physical input.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
