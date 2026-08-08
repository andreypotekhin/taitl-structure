# Search Passage Presentation


`SearchPassages` presents paragraph-level lexical evidence with enough local context for a caller to cite or assemble
an answer context.


The transform accepts the original documents plus paragraph and section boundaries. Each result contains the matched
paragraph, document title and URL, section heading, lexical score, and nullable
preceding and following paragraph content. Context may include only adjacent paragraphs in the same document section.

Only the matched paragraph contributes terms and rank. Adjacent matched paragraphs remain separate results, and the
caller chooses top-K, overlap removal, citation policy, and answer assembly.

## How it works

Paragraphs are the initial passage grain because they preserve source context without imposing an answer model. Context
stays within a section so unrelated headings cannot be joined. Adaptive chunking and configurable context radii remain
future choices.


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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Result grain | Document; sentence; passage | Passage + parent | Context stays without answers. |
| Radius | Whole section; fixed local radius; caller text | Fixed local radius | Work and output size remain bounded. |
| Result policy | Score context; no context; display-only context | Display-only context | Context cannot change rank. |


Failures must report center and neighbor identity, section boundary, radius, and snapshot. Examples should include
first/last passages, missing neighbors, multiple sections, and reordered physical input.
