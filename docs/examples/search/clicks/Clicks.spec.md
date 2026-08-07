# Search Clicks


`Clicks` attributes user actions to displayed impressions and publishes daily engagement facts for later feedback
modeling. It does not decide whether a click proves relevance.


Each click has an immutable ID, impression ID, occurrence time, and dwell duration. A click is accepted only when its
impression exists and the occurrence is between display time and 24 hours after display. Orphan, duplicate, late, and
out-of-window clicks produce no attributed fact. Repeated clicks on one impression remain separate engagement events,
while later CTR logic counts the impression only once.

Daily facts retain click count, clicked-impression count, dwell, dwell credit, and long-click count. Attribution uses
the
impression's display window, so a midnight boundary cannot move engagement to an unrelated exposure day.

## Design

The bounded impression join was chosen over an unbounded click lookup. A 24-hour attribution interval was chosen over
same-day attribution because the exposure, not the click calendar day, is causal context. Raw click count remains beside
binary clicked-impression count so engagement and CTR answer different questions.


The caller owns stream lifecycle and durable idempotent writes. Acceptance requires bounded attribution, deterministic
deduplication, correct repeated-click semantics, and parity between supported execution modes.


The transform must reject or exclude orphan impressions, duplicate IDs, invalid intervals, and unusable propensity
values according to the registered diagnostic contract. Fixtures should include midnight-crossing attribution, repeated
clicks, orphan clicks, late clicks, and unclicked impressions.


| Concern | Contract |
|---|---|
| Join identity | Request/impression lineage |
| Time interval | A click is eligible only inside the configured attribution window and event-time policy. |
| State bound | The transform reads a snapshot of impressions and clicks; it does not mutate a live counter. |
| Output grain | At most one attributable engagement fact per declared click/impression key. |
| Engagement | Repeated clicks are retained as evidence but do not multiply a binary CTR outcome. |
| Late data | Late or orphan events follow an explicit reject/quarantine policy and remain diagnosable. |

The attribution relation must preserve the exposure denominator. A click without a matching impression is not
evidence that an exposure occurred, and an impression without a click remains a valid negative observation.
Event time, not ingestion order, determines eligibility; the snapshot boundary determines reproducibility.

The attribution predicate is therefore a bounded event-time join:

```text
eligible(click, impression) =
    click.impression_id = impression.id
    and impression.displayed_at <= click.clicked_at
    and click.clicked_at <= impression.displayed_at + 24 hours
```

The impression's display date owns the attributed fact. This is why a click just after midnight remains attached to
the exposure that produced it, rather than moving into an unrelated daily denominator.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Attribution | Alternatives in choices above | Request/impression lineage | Keeps attribution explainable |
| Click identity | Alternatives in choices above | Immutable event identity | Makes replay detection possible |
| CTR semantics | Alternatives in choices above | Binary per impression | Keeps CTR bounded |

Diagnostics should expose unmatched IDs, outside-window events, duplicate conflicts, and invalid timestamps.
Fixtures must include midnight-crossing windows and replayed clicks so the result is proven independent of row
order and ingestion sequence.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
