# Search Document Profiling


`ProfileDocuments` derives reusable document attributes for analysis and optional training. It provides descriptive
features without changing the caller's source document contract.


Profile rows retain document identity and caller-owned metadata while adding normalized title/content facts, length and
date attributes, and simple classification indicators. The output is suitable for feature or analysis joins, not as a
replacement for the source document relation.

## Design

Profiling is separate from serving because descriptive attributes have different freshness and governance from ranking
evidence. A hidden profile-based boost was rejected. Profile values remain transparent inputs to an explicit feature or
training composition.


Profiles are deterministic for fixed source rows, retain tenant/corpus identity where supplied, and are optional to
chunking, indexing, and presentation.


| Feature family | Examples | Provenance |
|---|---|---|
| Structural | Section, paragraph, and sentence counts | Chunking policy and source snapshot. |
| Textual | Length, vocabulary size, language hints | Normalization policy and target identity. |
| Quality | Missingness, duplicate signals, parse status | Ingestion diagnostics and document identity. |
| Tenant/corpus | Tenant, corpus, document, source version | Caller-supplied scope keys. |

Profiles are descriptive facts at document grain. They do not replace the source hierarchy and do not become
ranking features merely because they are present. Optional providers must return compatible names, types, and
provenance so repeated profiling can be compared safely.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Role | Alternatives in choices above | Optional profile | Keeps lineage explicit |
| Source | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Freshness | Alternatives in choices above | Source snapshot | Keeps lineage explicit |

Failures should name document, feature, type, provider, and snapshot. Evidence must cover missing hierarchy,
empty text, repeated profiling, and omission from a ranking run.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
