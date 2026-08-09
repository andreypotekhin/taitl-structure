# Search Fields Model

`ExtractDocumentFields` preserves the existing typed `Document` values while completing the authoritative
`Document.fields` map. A map value wins when both representations provide the same reserved key; a typed value fills a
missing map key. The transform also exposes one flat `DocumentField` row per non-empty map key.

`FieldProfile` selects whether a field is searchable, whether it is text or keyword data, and whether phrases are
enabled. `AnalyzerPolicy` supplies versioned stop-word and normalization behavior. Arbitrary map keys use the dynamic
profile when one is configured, so callers can add searchable metadata without changing the `Document` schema.

The field model deliberately excludes body `content`. Body retrieval remains owned by `LexIndex` and participates in
field search only through an explicit `content:` query clause.
