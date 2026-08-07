# Search Impressions


`Impressions` turns displayed search results into daily exposure facts. It preserves the denominator needed for CTR and
exposure-aware feedback even when a displayed result receives no click.


Each input impression has an immutable ID, request identity, query text, document ID, displayed position, display time,
and caller-supplied examination propensity. IDs are deduplicated within the declared seven-day watermark. Query text is
normalized using the Search query contract.

The output is grouped by the impression display window and complete exposure identity. Every valid exposure contributes
to `impression_count`; a missing click never removes it. Events beyond the watermark may be discarded according to the
caller-owned stream policy.

## Design

An impression-backed contract was chosen over click-only aggregation because click-only data cannot measure exposure or
no-click requests. Propensity is logged by the serving system rather than inferred from position, because position is
not a calibrated examination model. Daily display windows, rather than click calendar days, anchor later attribution.


The caller owns the input stream, watermark delay, checkpoint, trigger, sink, and restart behavior. Acceptance requires
duplicate IDs to be idempotent, unclicked impressions to remain visible, and output keys to be stable across retries.


Invalid or nonpositive propensities, missing event identity, duplicate conflicting IDs, and inconsistent request fields
must not silently produce feedback. Evidence should prove unclicked exposures, duplicate replay, optional user context,
and stable daily keys.


| Concern | Contract |
|---|---|
| Event identity | Request, impression, result, document, tenant, and event-time keys are preserved. |
| Required evidence | An impression must identify what was shown and under which query/ranking snapshot. |
| Propensity | Positive, finite propensity is required when inverse-propensity use is enabled. |
| State bound | Exposure rows represent a declared event snapshot, not a mutable online log. |
| Output grain | One exposure fact per impression identity; duplicate replay is idempotent or rejected. |
| Zero-click | An unclicked exposure remains in the output and contributes to the denominator. |

An impression is evidence of exposure, not evidence of relevance. The transform therefore keeps ranking
position, policy/version identity, and request lineage even when no engagement follows. Optional user context
must not be required for anonymous or privacy-restricted impressions.

The exposure denominator is defined before engagement is joined:

```text
CTR_denominator(query, day) = count(valid impressions shown on day)
CTR_numerator(query, day)   = count(impressions with at least one attributed click)
```

An impression with no click contributes to the denominator and remains available for later propensity-weighted
aggregation. Propensity is logged by the serving system rather than inferred from rank alone because the same rank can
be shown in different layouts, devices, and candidate populations.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Exposure evidence | Alternatives in choices above | Served-impression rows | Keeps lineage explicit |
| Time grouping | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Propensity | Alternatives in choices above | Optional, validated field | Keeps lineage explicit |

Failures must distinguish missing identity, conflicting replay, invalid propensity, and malformed request
lineage. Evidence should prove stable daily keys, empty-click retention, and identical output under reordered
input rows.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
