# Search Query Labeling


Labeling enriches queries with caller-defined and deterministic intent labels so evaluation can compare meaningful query
populations without making labels ranking inputs.


Caller labels are timestamped and merged by latest value. Intent catalogs provide stable label names; locale-specific
regular-expression patterns create optional derived labels. Different requested label names are combined with AND,
multiple values for one name are alternatives, and an empty selection keeps every query. Missing language falls back to
the documented default locale.

Labels remain on `SearchQuery` and are carried through evaluation selection. They do not change lexical normalization,
candidate admission, feedback, or ranking.

## Design

Labels were chosen as evaluation metadata rather than hidden ranking features. A language-understanding model was
rejected for the example because it would make slices nondeterministic and provider-dependent. Caller labels take
precedence over derived labels for the same named field.


Latest labels are deterministic, locale fallback is explicit, label predicates implement AND/OR semantics correctly,
and removing labels leaves search results unchanged.


| Concern | Contract |
|---|---|
| Label identity | A label is keyed by target, label name, locale, and source snapshot. |
| Merge order | Sources have deterministic precedence; duplicate conflicts are diagnosable. |
| Locale | Exact locale, fallback locale, and no-label outcomes are distinguishable. |
| Predicate | AND/OR semantics are declared and independent of row order. |
| Ranking effect | Labels constrain eligibility only unless a separate policy opts in. |
| Snapshot | Label facts match the document/corpus snapshot they filter. |

Labels are metadata constraints, not a second relevance model. A target can have multiple labels, but the
resolved relation preserves source and locale provenance. Removing a label changes eligible results only when the
caller explicitly applies that predicate.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Source | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Pattern semantics | Alternatives in choices above | Declared operators | Keeps lineage explicit |
| Ranking use | Alternatives in choices above | Hard filter boundary | Keeps lineage explicit |

Diagnostics should name target, label, locale, source, and precedence. Evidence must cover locale fallback,
conflicting sources, AND/OR predicates, and label removal with unchanged baseline ranking.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
