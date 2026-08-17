# Search Offline Preparation

Offline preparation builds reusable Search artifacts from bounded, caller-owned snapshots. It selects the query
population, prepares filter targets, fills the complete vector population when configured, and produces timestamped
score and candidate relations for later serving.

The offline path is deliberately explicit about population and lineage. Popular queries are bounded, recent queries
use the active score policy window, and the merged query set is deduplicated before scoring. Document vector
preparation covers the complete document population; the caller owns persistence of the resulting embeddings and
artifacts.

## How it works

- Select popular and recent queries using normalized query identity and deterministic ties.
- Run filtering for the selected query population and publish bounded document targets.
- Run offline vectorization with `streaming=False` for all documents and selected queries.
- Merge compatible cached and inferred embeddings under the active inference/vector policy.
- Run shared scoring and publish lexical/vector score artifacts with snapshot timestamps.

Offline artifacts are inputs to online gap resolution. Their validity depends on query identity, target scope, policy
effective time, model compatibility, and corpus snapshot; a newer or incompatible row cannot silently become serving
evidence.

| Artifact | Population | Purpose |
|---|---|---|
| Query set | Popular plus recent | Bound offline computation while covering current demand. |
| Filter scores | Selected query set | Establish document admission and target scope. |
| Vector embeddings | Complete document population and selected queries | Provide reusable vector evidence. |
| Lexical/vector scores | Selected query and target populations | Provide inspectable serving artifacts. |

Diagnostics should include query selection, snapshot identity, policy timestamps, cache compatibility, and provider
statuses. Evidence should cover popularity ties, recent-window boundaries, duplicate queries, empty populations,
failed inference, and deterministic artifact reuse.
