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

## How it works

Clicks are attributed through a bounded impression join. The 24-hour interval is anchored to exposure time rather than
the click calendar day, and raw click count remains beside binary clicked-impression count so engagement and CTR answer
different questions.


The caller owns stream lifecycle and durable idempotent writes. A conforming implementation preserves bounded
attribution, deterministic deduplication, correct repeated-click semantics, and parity between supported execution
modes.


The transform must reject or exclude orphan impressions, duplicate IDs, invalid intervals, and unusable propensity
values according to the registered diagnostic contract. Examples should include midnight-crossing attribution, repeated
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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Attribution | Click day; unbounded; impression lineage | Request/impression lineage | Lineage stays clear. |
| Click identity | Arrival order; mutable counter; event ID | Event ID | Replays are detectable. |
| CTR semantics | Raw; repeats; binary impression | Binary per impression | CTR stays bounded. |


Diagnostics should expose unmatched IDs, outside-window events, duplicate conflicts, and invalid timestamps.
Examples should include midnight-crossing windows and replayed clicks so the result is proven independent of row
order and ingestion sequence.
