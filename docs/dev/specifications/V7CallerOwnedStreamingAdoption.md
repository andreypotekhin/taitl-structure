# V7 Caller-Owned Streaming Adoption

## Purpose

V7 expands streaming transformations in independently verified stages while callers retain source, sink, checkpoint,
trigger, output-mode application, and query lifecycle ownership.

## Stage One: Stream-Static Enrichment

The current relation is streaming and remains on the left; the lookup relation is explicitly static. V7 admits typed
inner, left, and left-semi joins. Projecting lookup fields requires a unique lookup key or an existing deterministic
dedupe policy. The stage is stateless, requires no watermark, and inherits the caller's output-mode decision.

## Stage Two: Left-Outer Static Lookup

V7 admits left-outer stream-static lookup only after Stage One live evidence passes. Unmatched streaming rows remain in
the result; lookup fields are declared nullable. Right/full/cross/anti directions and a streaming lookup relation are
rejected before query start.

## Stage Three: One Stateful Operation Then Stateless Work

A step or composed pipeline may contain exactly one already-admitted stateful operation—watermarked dedupe,
window/session aggregate, or bounded stream-stream join—followed only by row-local projection/filtering or a supported
stream-static enrichment. The original stateful operation retains its watermark, bound, cardinality, and required output
mode. A second stateful operation, generator, ordering/limit, analytic window, or selected-row helper is batch-only.

## Shared Rules

Explain identifies input modes, stateful operation (when present), watermark/bound facts, and any required output mode.
Generated source must not contain `readStream`, `writeStream`, checkpoint, trigger, output-mode calls, `start`, query
lifecycle calls, actions, RDD/Pandas conversion, or UDF fallback. Diagnostics name the incompatible shape and a
caller-owned alternative.

## Evidence

Each admitted stage has a test-owned file stream that processes input, writes to a test-owned sink, stops, and restarts
with the same test-owned checkpoint. Online and generated forms must produce equivalent plans and results on PySpark
3.5 and 4.0. A target disagreement or restart discrepancy leaves that precise stage unsupported.
