# Search Query Labeling


Labeling enriches queries with caller-defined and deterministic intent labels so evaluation can compare meaningful query
populations without making labels ranking inputs.


Caller labels are timestamped and merged by latest value. Intent catalogs provide stable label names; locale-specific
regular-expression patterns create optional derived labels. Different requested label names are combined with AND,
multiple values for one name are alternatives, and an empty selection keeps every query. Missing language falls back to
the documented default locale.

Labels remain on `SearchQuery` and are carried through evaluation selection. They do not change lexical normalization,
candidate admission, feedback, or ranking.

## How it works

Labels are evaluation metadata rather than hidden ranking features. Deterministic caller and pattern sources keep slices
reproducible; a language-understanding model would make the example provider-dependent. Caller labels take precedence
over derived labels for the same named field.


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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Source | Model labels; caller labels; caller + patterns | Caller + patterns | Slices stay deterministic. |
| Pattern semantics | OR predicates; arbitrary; declared operators | Declared operators | Operators stay explainable. |
| Ranking use | Hidden feature; request filter; evaluation | Hard filter boundary | Labels stay out of ranking. |


Diagnostics should name target, label, locale, source, and precedence. Examples should cover locale fallback,
conflicting sources, AND/OR predicates, and label removal with unchanged baseline ranking.
