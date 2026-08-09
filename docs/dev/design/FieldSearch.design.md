# Design: Field-aware boolean and phrase search

## Status and Authority

This document defines the design for field-aware boolean and phrase search in the Search example. The implementation
plan is docs/dev/planning/P08082603.Field-aware-boolean-and-phrase-search.plan.md. The Search specification and
implementation must be updated when this design becomes active.

## Purpose

Search currently provides lexical retrieval over document content through LexIndex, BM25, overlap, and the existing
document-search funnel. This design adds a separate, inexpensive metadata search lane.

The new lane supports boolean term constraints over document fields, positional phrases such as
title:"release notes", arbitrary caller-defined string fields, a metadata default scope for unqualified terms, and
explicit content: clauses that reuse existing full-text content search.

The design deliberately does not create a positional index for full document content. Content phrases are not part of
this slice.

## Ownership and Scope

The caller owns the document source, the authoritative values in Document.fields, field-definition and analyzer-policy
snapshots, persistence, query serving, and index refresh. Structure owns typed field extraction, field indexing, query
parsing, and DataFrame transformations.

Search remains an example application rather than a hosted search service. It does not own a crawler, metadata store,
model invocation, query server, or streaming index-refresh lifecycle.

## Two Retrieval Lanes

The application has two independent indexing lanes:

    Document.content
        -> Chunking
        -> LexIndex
        -> existing full-text scoring and document search

    Document
        -> Chunking + ExtractDocumentFields
        -> Indexing
           -> LexIndex
           -> FieldIndex
        -> boolean and phrase metadata matching

LexIndex remains the full-text content index. FieldIndex is a new field-aware child of Indexing. Shared tokenization and
normalization helpers may be reused, but the public artifacts remain separate because their evidence is different:
LexIndex supports term-frequency scoring, while FieldIndex supports field identity and positions.

Body content is not copied into Document.fields, is not processed by FieldIndex, and is not automatically searched when
metadata produces no results.

## Document Source Model

Document retains all existing plain schema fields. It gains an authoritative non-null map:

    fields: map<string, string>

The map contains reserved string metadata keys and arbitrary custom keys. The body content field remains a dedicated
top-level field because it is large text with a separate chunking and lexical-index lifecycle.

The reserved metadata keys are:

    title
    url
    source
    content_type
    encoding
    language
    document_type
    category_id
    file_type

content_summary is intentionally not included. It can be added later as an explicit caller-owned derived field if
experience shows that a short synthesized field is valuable.

The map is the source of truth. Existing named Document fields remain present because they are useful to callers and
presentations, but ExtractDocumentFields assigns their values from the map in its enriched Document output. When a
typed value and map value both exist, the map value wins; downstream consumers therefore use one consistent value.

Non-string identity and lifecycle fields, such as document ID, collection ID, and timestamps, remain typed fields rather
than being stringified into the map.

## ExtractDocumentFields

ExtractDocumentFields follows Chunking. Chunking establishes content structure; extraction owns metadata
canonicalization and flattening while enriching the same `Document` relation.

Its dataflow is:

    Document
        -> Chunking
            -> ExtractDocumentFields
            -> enriched Document
            -> DocumentField rows

The enriched Document preserves every existing plain field. Its named string fields are assigned from the authoritative
map, while identity, lifecycle, and body-content values are copied from the source row.

DocumentField is the distributed relational view used by FieldIndex. Each row contains:

    document_id
    field_name
    field_value
    field_kind
    analyzer_policy
    ordinal

ordinal is deterministic and is useful if a future source permits multiple values per field. The initial map has one
value per key. Field extraction rejects null or empty field names and does not tokenize body content.

## FieldProfile and AnalyzerPolicy

FieldProfile answers: “What is this field and where can it be searched?” It contains or references the field name or
dynamic-field default, field kind such as text or keyword, whether the field is searchable, whether phrase matching is
enabled, the default query scope, and the referenced analyzer policy.

AnalyzerPolicy answers: “How are values and query text transformed?” It is a stable versioned identity for tokenization,
case normalization, punctuation handling, stop-word removal, spelling/morphology/synonym rule tables, and position
behavior.

The policy identifier and version are persisted with field artifacts. The rules themselves are not duplicated into every
posting row.

Examples:

    Field: title
    Kind: text
    Analyzer: metadata_text_v1
    Phrase behavior: positional phrase

    Field: source
    Kind: keyword
    Analyzer: keyword_lowercase_v1
    Phrase behavior: one logical position; exact normalized value

    Field: category_id
    Kind: keyword
    Analyzer: keyword_exact_v1
    Phrase behavior: one logical position; exact value

    Field: file_type
    Kind: keyword
    Analyzer: keyword_lowercase_v1
    Phrase behavior: one logical position; exact normalized value

    Dynamic custom field default
    Kind: text
    Analyzer: metadata_text_v1
    Phrase behavior: positional phrase

Initial analyzer policies are:

    metadata_text_v1
        deterministic tokenization
        case normalization
        punctuation normalization
        configured stop-word removal
        original-position preservation

    keyword_lowercase_v1
        trim the complete value
        case-normalize the complete value
        keep it as one logical token

    keyword_exact_v1
        trim the complete value
        preserve the normalized value as one logical token

All fields in Document.fields are phrase-enabled by default. Keyword fields still have a position, but their normal
query behavior is exact normalized matching. A field definition can disable search or override its analyzer without
changing the behavior of other fields.

Custom spelling, morphology, and synonym tables are caller-owned, versioned policy inputs. They must be applied
identically to field values and field-qualified query values. An opaque analyzer UDF or external NLP service is outside
this design.

## FieldIndex

FieldIndex consumes DocumentField rows and emits positional field postings:

    document_id
    field_name
    term
    position
    analyzer_policy

Each analyzed occurrence remains a separate posting. Removing a stop word does not renumber later positions. Therefore,
the indexed value history of art retains positions for history and art with a gap between them. A phrase query with the
same analyzed stop-word gap can match; history art does not become equivalent unless a future slop policy explicitly
allows it.

FieldIndex does not compute BM25 or overlap statistics. Field matching is a document constraint. If a query also has a
content: clause, the existing full-text score supplies ranking evidence.

The body-content cost boundary is intentional:

- content boolean matching can reuse document-level terms from LexIndex;
- content full-text scoring reuses the current scoring path;
- metadata fields receive compact positional postings;
- no content-position artifact is created.

## FieldSearchQuery

FieldSearchQuery is the query input for field-aware search. It carries the normal query identity, queryset, raw query
text, immutable request time, and language/query metadata needed by Search.

Canonical query syntax uses lowercase operators:

    title:"release notes"
    source:github
    category_id:docs
    title:"release notes" and content:upgrade
    title:guide or source:github

The parser requires lowercase operator tokens and rejects uppercase or mixed operators. A field name is an identifier
followed by a colon. A value is one term or a quoted phrase. Whitespace between unqualified terms is an implicit `and`.

Unqualified terms and phrases use the metadata default scope:

    release notes

This does not search body content. Body content participates only when the query contains content:.

content: is reserved and is not a Document.fields key. Its value is passed to the existing full-text query and scoring
semantics. Quotes around a content value group the value for the field clause; they do not create content phrase
semantics in this design.

Metadata-only expressions support and and or. Mixed metadata/content expressions support and:

    title:"release notes" and content:upgrade

This means “the document satisfies the title phrase and receives a full-text content match for upgrade.” The metadata
target set is intersected with content-scored document IDs. Existing full-corpus lexical statistics and content scores
are preserved.

Mixed or is rejected in this slice because it needs an explicit ranking policy for metadata-only, content-only, and
both-lane matches. There is no metadata-to-content fallback.

## SearchFields

SearchFields is the serving composition for this lane. Its name describes the behavior without implying that it replaces
SearchDocuments.

Callers parse raw text with `parse_field_search_query`, materialize the resulting `FieldSearchQuery` and
`FieldSearchTerm` rows, and pass those typed rows to `SearchFields`. `SearchFields` analyzes metadata clauses with their field definitions, matches terms or
positions against FieldIndex, applies boolean logic, and emits deterministic document-ID order.

For a mixed and, it additionally converts the content: clause to the existing SearchQuery/scoring path, optionally
restricts content scoring to metadata candidate IDs for efficiency, intersects metadata matches with content-scored IDs,
and ranks the result using the existing content score and deterministic document-ID ties.

The metadata lane does not invent a relevance score. A metadata-only result is a constraint match. A content clause
provides the lexical ranking signal.

## Performance and Storage

The current LexIndex stores compact term-frequency rows, approximately one row per distinct term per target. A
phrase-capable metadata index stores one row per analyzed field-term occurrence, but metadata values are much shorter
than body content.

Avoiding a body positional index is therefore the main storage boundary. FieldIndex remains limited to the map-backed
metadata fields. Body boolean lookup can use existing document term rows, and body relevance uses existing BM25 and
overlap calculations.

The implementation should record field-token counts and posting counts in tests or benchmark notes. The representative
comparison is metadata posting count versus total content occurrence count, not merely the number of documents.

## Diagnostics and Failure Behavior

Diagnostics must identify the query ID, field name, policy identifier, and remedy where applicable. They must cover null
or empty field names, unsupported or conflicting field profiles, malformed field qualification, malformed quoted phrases,
unsupported mixed or, policy-version mismatch, and invalid rewrite rules that cannot preserve positions.

Absent custom keys simply produce no matching field rows. They must not be interpreted as body content.

## Testing and Evidence

Evidence must prove that Document.fields is authoritative while all existing named Document fields remain present and are
assigned during extraction; reserved and arbitrary fields flatten deterministically; stop-word removal preserves phrase
gaps; title and custom-field phrases match positionally; unqualified queries search metadata only; content:upgrade
preserves the existing full-text result behavior; title:"release notes" and content:upgrade intersects metadata matches
with full-text results; no content-position index is generated; metadata-only results have deterministic document-ID
ordering; mixed results retain existing content scores; and online and generated execution have equivalent schemas and
results.

## Deferred Work

This design does not add content phrase search, content summaries, mixed or, streaming field-index maintenance, answer
generation, external NLP services, or changes to the existing LexIndex scoring formulas.
