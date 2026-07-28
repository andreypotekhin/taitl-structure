# V7 Caller-Owned Streaming Adoption Gate

## Decision

V7 adopts streaming in three independently verified stages. It first admits caller-owned stream-static enrichment,
then extends it to left-outer lookup semantics, then design-gates one existing stateful operation followed only by
stateless transformations. Structure continues to return only a transformed DataFrame; callers own the source, sink,
checkpoint, trigger, output mode, and query lifecycle.

## Why This Is the Next Gate

V4 already admits watermarks, bounded stream-stream outer/semi joins, session-window aggregation, and stream-static
semi filtering. The next useful adoption step should make a common enrichment shape available without introducing new
state or operational ownership. Unlike stream-stream work, stream-static enrichment has no cross-stream retained state;
its main risks are join direction, row multiplication, lookup-key uniqueness, and preserving the streaming side.

## Candidate Contract

The current relation must be streaming and remain on the left. The lookup relation must be explicitly static. A typed
left lookup join may expose lookup fields only when the declared lookup key is unique or a deterministic existing
dedupe policy selects one row. An inner join may filter unmatched streaming rows. A left-semi existence filter remains
the already supported non-projecting alternative. Right, full, cross, anti, and a streaming lookup side are rejected
with a diagnostic naming the safe left-oriented form.

The first candidate is stateless. It requires no watermark and imposes no output-mode requirement beyond what the
caller's source/sink combination already requires. Its explain record must still state that the current side is
streaming and the lookup side is static, so a later source-mode change is rejected before query start.

## Feasibility Evidence

Create an isolated file-stream test fixture that writes a small input batch, applies the candidate transform, writes to
a test-owned memory or file sink, then stops and restarts the query with the same test-owned checkpoint. Prove that the
transform itself emits no `readStream`, `writeStream`, `trigger`, checkpoint, output-mode, `start`, action, or query
lifecycle call. Run the fixture on PySpark 3.5.x and 4.0.x.

Stage two admits left-outer stream-static lookup only after the same evidence proves unmatched streaming rows retain
their declared nullable lookup fields. Stage three permits exactly one existing stateful operation—watermarked dedupe,
window/session aggregate, or bounded stream-stream join—before stateless projection/filter/enrichment operations. It
must not add another stateful operator, reorder rows, or change the stateful operation's documented output-mode rule.

Each stage fails closed if either target rejects the plan, lookup duplication lacks a deterministic Structure contract,
or restart behavior differs. The rejected shape remains caller-owned raw PySpark and does not authorize a broader claim.

## Non-goals

This program does not admit generator operations to streaming, two or more stateful operations, sorting, limits, analytic
windows, stream-static right/full/cross/anti joins, source/sink ownership, `foreachBatch`, or Spark Connect streaming.

The normative behavior is [V7 Caller-Owned Streaming Adoption](../specifications/V7CallerOwnedStreamingAdoption.md).
