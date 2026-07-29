# V9 PySpark Streaming API Coverage

## Purpose

V9 expands Structure from parity over streaming-compatible transformations to broad, checked coverage of the PySpark
Structured Streaming API surface. The release should help a developer answer three questions from one place: which
streaming APIs Structure models as typed transformations, which APIs remain caller-owned operational code, and which
APIs are rejected because they would hide unsafe state, side effects, or lifecycle ownership.

V9 does not make Structure a streaming job orchestrator by default. A Structure transform still returns a PySpark
DataFrame plan. Sources, sinks, checkpoints, triggers, output modes, query names, starts, stops, deployment, and
recovery remain caller-owned unless a later explicit product decision creates a separate lifecycle-owning runtime.

## Coverage Ledger

V9 introduces a checked PySpark streaming API ledger at
`src/structure/plugin/pyspark/resources/pyspark-streaming-api-coverage.json` beside the existing transformation and
Structured Streaming coverage ledgers. The ledger classifies API families rather than only transformation families. It
must include at least these groups:

- streaming input declarations and input-mode diagnostics;
- DataFrame streaming metadata such as `isStreaming`;
- watermarks and event-time bounds;
- stateful transformations, including aggregations, dedupe, session windows, stream-stream joins, and composition
  limits;
- stateless streaming transformations inherited from v8;
- DataStreamReader sources such as file, table, rate, and Kafka;
- DataStreamWriter sinks such as memory, console, file, table, and Kafka;
- writer options including output modes, checkpoint locations, query names, triggers, partitioning, and format
  options;
- query lifecycle APIs such as `start`, `stop`, `awaitTermination`, status, progress, and exception inspection;
- side-effect APIs such as `foreach`, `foreachBatch`, and listener callbacks;
- arbitrary state APIs, RDD/Pandas boundaries, and Spark Connect streaming.

Each row must state one status:

- `structure-supported`: Structure models the API through typed source, generated PySpark, online execution,
  diagnostics, explain/traceability, and live evidence.
- `caller-owned-guided`: Structure does not generate or execute the API, but docs, diagnostics, examples, and tests
  show the caller-owned integration point.
- `design-gated`: the API is plausible, but implementation waits for a state, lifecycle, or side-effect design.
- `streaming-ineligible`: Spark or Structure cannot support the shape without violating the stated contract.
- `out-of-scope`: the API belongs to deployment, cluster operations, or a future product direction outside v9.

The ledger is not merely documentation. A guard test must prove that every selected PySpark streaming family is
classified, every evidence path exists, and no lifecycle API is accidentally counted as a transformation claim.

## Adoption Coverage

V9 should make caller-owned streaming easier without hiding ownership. Public adoption material must include:

- a file-stream transform example with restart from a caller-owned checkpoint;
- an example that uses a caller-owned source and sink around generated Structure code;
- an output-mode guide for admitted stateful transformations;
- diagnostics that name the required caller action, such as adding a watermark, declaring a side input as streaming, or
  keeping a sink outside Structure;
- generated-source scans proving Structure does not emit `readStream`, `writeStream`, `start`,
  `awaitTermination`, checkpoint, trigger, output-mode, action, RDD, Pandas, or hidden UDF calls in compatible
  transform modules.

When v9 documents caller-owned APIs, the examples must compile and run in tests or live integration lanes. A prose-only
recipe is not enough for a coverage claim.

## Deferred v7 and v8 Items

V9 re-evaluates deferred streaming-related items from v7 and v8 under current evidence:

- stream-static and stream-stream join adoption gaps, including examples and diagnostics for declared input modes;
- one-stateful-plus-stateless composition, including clearer rejection of a second stateful operation;
- distinct-style relation set operations, which stay rejected unless Spark restart evidence proves a bounded shape;
- global ordering, limits, offsets, and priority selection, which remain batch-materialization boundaries unless a
  separate state contract is designed;
- selected-row helpers, analytic ranking windows, lag/lead, and rolling windows over streaming inputs;
- `foreach`, `foreachBatch`, and streaming side effects;
- Spark Connect streaming, which remains unclaimed unless a separate target contract proves support.

Non-streaming retained backlog from v7, such as Search evaluation follow-ups, plugin-owned DSL completion, incremental
compile cache diagnostics, and data-quality constraints, remains outside v9 unless a v9 streaming adoption slice needs
it directly.

## Admission Rules

An API can move to `structure-supported` only when all of these are true:

- the source syntax is typed and compiler-visible;
- schema, cardinality, state, output-mode, and lifecycle effects are explicit;
- unsupported shapes fail before query start with a corrective diagnostic and a documentation link;
- online execution and generated PySpark use the same lowered recipe;
- generated code is deterministic and does not emit lifecycle, action, RDD, Pandas, or hidden UDF calls unless the API
  is intentionally a caller-owned example outside generated transforms;
- PySpark 3.5 and 4.0 live evidence passes for the admitted shape.

An API can move to `caller-owned-guided` only when Structure has no hidden implementation path for it and the
documentation shows exactly where user code owns the PySpark call.

## Acceptance

V9 is complete when the streaming API ledger is checked, every selected PySpark Structured Streaming API family has a
status and evidence path, admitted transformation APIs have live PySpark 3.5/4.0 evidence, caller-owned lifecycle APIs
have runnable adoption examples, diagnostics clearly separate Structure-owned transformations from caller-owned
operations, and the final hardening sprint passes `make build`.

## Diagnostics and Troubleshooting

Streaming diagnostics must name the owner boundary. If the fix is a transform rewrite, the diagnostic names the missing
watermark, event-time bound, input-mode declaration, or unsupported stateful composition. If the fix is lifecycle
placement, the diagnostic tells the user to keep `readStream`, `writeStream`, checkpoints, triggers, output modes,
query lifecycle, `foreach`, and `foreachBatch` in caller-owned PySpark code such as `examples/streams/adoption.py`.
Troubleshooting guidance lives in `Troubleshooting.md`.
