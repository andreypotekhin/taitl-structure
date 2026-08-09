# Search Fields

`SearchFields` resolves inexpensive boolean and phrase constraints over extracted document metadata. It consumes the
field query relation and positional `FieldIndex` postings, then optionally intersects those metadata matches with the
existing full-text score relation.

## Query scope

Unqualified terms use the `metadata` default scope. A field prefix narrows matching to one field:

```text
release notes
title:"release notes"
source:github
```

Metadata text is normalized with the field analyzer. Stop words are removed from postings and query terms while their
original positions remain meaningful, so a phrase can preserve gaps such as `release the notes`.

The parser accepts lowercase `and` and `or`. Mixed operators are rejected. Metadata-only `or` returns documents that
match at least one clause; `and` requires every metadata clause to match the same document.

## Explicit content delegation

Body content is searched only when the query contains `content:`. Its value is passed to the existing full-text scoring
path rather than indexed into the positional field lane:

```text
title:"release notes" and content:upgrade
```

The metadata candidate set is intersected with content-scored document IDs. A metadata query never silently retries
against body content when it has no metadata matches, and no content-position index is created.

## Phrase matching

`FieldIndex` records one posting per surviving field token with its original field-local position. `SearchFields`
groups matching query terms by position offset; a phrase succeeds only when all of its terms share an offset. This
keeps phrase behavior field-local and preserves stop-word gaps without building positional metadata for document body.

## Results and boundaries

Metadata-only results are constraint matches with a zero score and `metadata` scope. Content-only results use the
existing full-text score and `content` scope. Mixed `and` results use the full-text score after metadata intersection
and carry `metadata+content` scope. Field search does not replace `SearchDocuments`; it supplies a focused constraint
and optional content-scoring lane.

Failures should identify the query, field, clause, analyzer policy, or unsupported operator and explain the remedy.
