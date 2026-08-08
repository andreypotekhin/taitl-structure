# Search Document Profiling


`ProfileDocuments` derives reusable document attributes for analysis and optional training. It provides descriptive
features without changing the caller's source document contract.


Profile rows retain document identity and caller-owned metadata while adding normalized title/content facts, length and
date attributes, and simple classification indicators. The output is suitable for feature or analysis joins, not as a
replacement for the source document relation.

## How it works

Profiling is separate from serving because descriptive attributes have different freshness and governance from ranking
evidence. Profile values remain transparent inputs to an explicit feature or training composition; they do not become a
hidden profile-based boost merely because they are available.


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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Role | Serving boost; opaque enrichment; optional profile | Optional profile | Profiles stay descriptive. |
| Source | Reparse everywhere; driver collection; source | Source boundary | Provenance stays visible. |
| Freshness | Wall clock; arbitrary latest; source snapshot | Source snapshot | Freshness follows source state. |


Failures should name document, feature, type, provider, and snapshot. Examples should cover missing hierarchy,
empty text, repeated profiling, and omission from a ranking run.
