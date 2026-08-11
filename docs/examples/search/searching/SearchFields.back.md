# Search Fields

`SearchFields` is the independent field-aware entry point. It resolves inexpensive boolean and phrase constraints over
explicitly prefixed metadata clauses, then delegates any body-content portion to the existing document-search funnel.
It consumes the field query relation and positional `FieldIndex` postings; it does not make `SearchDocuments` parse or
understand field syntax.

## Query scope

Only an explicit field prefix addresses metadata. The reserved `meta:` prefix searches one generated field containing
all searchable metadata values:

```text
title:"release notes"
source:github
meta:guide
```

The generated `meta` value uses deterministic source-field order and a positional gap between source fields. Therefore
`meta:"alpha beta"` cannot match `alpha` at the end of one original field followed by `beta` at the beginning of
another. Metadata text is normalized with the field analyzer. Stop words are removed from postings and query terms
while their original positions remain meaningful, so a phrase can preserve gaps such as `release the notes`.

Unprefixed terms are document body text:

```text
aurora beacon
```

`content:` is an explicit spelling for the same body lane. The parser accepts lowercase `and` and `or`; mixed
operators are rejected. Metadata-only `or` returns documents that match at least one clause; metadata/body `or` is not
supported (because it would require a ranking policy across the two evidence lanes).

## Explicit content delegation

Body content is searched whenever the parsed query refers to body text - with an explicit `content:`
clause or unprefixed text. Its value is passed to the full-text document search.
The metadata search results become a pre-filtering step before the delegated full-text query: 

```text
title:"release notes" and content:upgrade
```

A metadata-only query (without body text) never invokes the full-text path.

## Phrase matching

`FieldIndex` records one posting per surviving field token with its original field-local position. `SearchFields`
groups matching query terms by position offset; a phrase succeeds when all of its terms share an offset. This
keeps phrase behavior field-local and preserves stop-word gaps without building positional data for document body.

## Results and boundaries

Content-only results use the existing full-text score and `content` scope. 
Mixed query results use the full-text score after metadata filtering and carry `metadata+content` scope.
Results from delegated search carry the child `DocumentSearchResult` as an optional nested result. 
Field search does not replace `SearchDocuments`; it supplies field-aware boundary to it.
