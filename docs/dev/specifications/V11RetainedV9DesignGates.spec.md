# V11 Retained V9 Design Gates Specification

## Scope

This specification defines the observable status and boundary contract for V9 items retained into V11. It admits a
typed, caller-started row-sink handoff for final outputs. Row-based `transform_with_state(...)` is separately admitted
by the V11 row-state contract; this specification does not widen that claim to Pandas or Dataset/Scala processors. It
does not admit Variant mutation, XML helpers, or join reordering.

## Normative rules

1. Each retained item has exactly one V11 status: `design-gated`, `caller-owned-guided`, `streaming-ineligible`, or
   `unsupported`.
2. A `design-gated` item must fail before lowering with a diagnostic that names the item, explains the missing contract,
   and gives the caller-owned remedy.
3. For `caller-owned-guided` items, runtime lifecycle and side-effect calls remain caller-authored. Generated Structure
   modules must not contain their lifecycle or side-effect calls.
4. A target-specific row must name its required PySpark profile. An unavailable runtime is unavailable evidence, never a
   passing result.
5. The exact admitted two-stage event-time window shape remains unchanged. Other stateful chains remain rejected until
   their state-stage contract is accepted.
6. XML helpers and cost-based join reordering remain unexported and unsupported for V11.

## Required retained-gate contracts

| Item | Status | Required rejection or handoff |
| --- | --- | --- |
| Variant mutation helpers | `design-gated` | Reject mutation lowering unless a released 4.3+ profile and complete path/type/null contract are available. |
| `is_valid_variant(...)` | `design-gated` | Require the PySpark 4.2 capability; report unavailable live evidence separately from rejection evidence. |
| Chained stateful operators | `design-gated` | Admit only the proven event-time window pair and, on ordinary PySpark 3.5/4.0, watermarked dedupe followed by one watermarked event-time window aggregate in Append mode. Require per-operator budget declarations when configured; reject all other pairs with prior/requested stage context. |
| Row-level `foreach` | `caller-owned-guided` | A transform may declare a typed sink and capture `foreach(row, sink)` on a final output. The caller constructs the writer, attaches it to the returned DataFrame, and owns query lifecycle, checkpoints, identity, retries, and recovery. |
| `foreachBatch` | `caller-owned-guided` | A transform may declare a schema sink and return a batch handoff. The caller supplies `ForeachBatchSafety`, runs the batch transform for each batch ID, writes externally, and owns retries, checkpoints, and idempotence. |
| Row-based `transformWithState` | `structure-supported` | Ordinary PySpark 4.1 supports Append/Update, None/ProcessingTime/EventTime in valid combinations, composite keys, timers, online/generated parity, and same-checkpoint restart. Reject Complete during compilation and reject this row operation on PySpark 4.0, where the Python API is Pandas-based. The typed path exposes one `ValueState`; native state features remain available through the opaque processor binding. |
| `transformWithStateInPandas` | `design-gated` | Feature-specific typed/native, timer, event-time, differential, output-mode, TTL, and tested checkpoint-evolution evidence exists for ordinary PySpark 4.0/4.1; the canonical 4.1 lane passes, while the complete 4.0 lane exceeded its deadline. Retain the gate until the complete 4.0 lane passes. |
| Legacy `apply_in_pandas_with_state` | `design-gated` | Keep the legacy typed/native compiler path separate from Spark 4 processor APIs; accumulation and restart are evidenced on ordinary PySpark 3.5/4.0/4.1, while timeout and zero/multiple-output behavior remains independently gated. |
| XML helpers | `unsupported` | Do not export or lower XML helpers. |
| Cost-based join reordering | `unsupported` | Do not export `join_order(...)` or reorder source-authored joins. |

## Acceptance evidence

The implementation is acceptable only when the catalog, machine-readable ledgers, diagnostics, generated-source scans,
and caller recipes agree with this table. Positive support for a previously gated row additionally requires a normative
contract, focused negative tests, online/generated parity, and live evidence for the claimed target and variant. The
row processor support claim does not extend to the separate Pandas interfaces, Spark Connect, PySpark 4.2, or general
stateful-operation chaining.

## Row-level `foreach`

Subclass `structure.plugin.pyspark.Sink` for a module-level writer whose Python implementation Structure does not inspect
or execute during compilation. The inherited role also guards calls to writer methods during step compilation. Declare
a named writer on a transform with `sink(WriterClass)`. A step takes the writer as a typed parameter; a unique declared
writer type binds automatically, while `@step(sink=declaration)` disambiguates
multiple declarations of the same class. The step calls `foreach(returned_row, writer_parameter)` and returns that same
row value. The row must map to a declared final output of that transform. Intermediate rows and rows created after the
sink call are rejected with `DSL-E0406`.

The result exposes a read-only named handoff directly, such as `result.publish_alerts`. It contains the matching final
output DataFrame as `.dataframe` and the writer class as `.writer`. Structure does not instantiate the writer, call its
methods, start an action or query, or attach it to the caller's existing output query. A batch caller passes
`handoff.writer(...).process` to `handoff.dataframe.foreach(...)`. A streaming caller passes a configured noncallable
writer instance to `handoff.dataframe.writeStream.foreach(...)` and starts the returned writer using ordinary PySpark
APIs.

The batch writer defines `process(row: pyspark.sql.Row) -> None`; it receives PySpark `Row` objects, not instances of
the declared Structure schema. Batch `DataFrame.foreach` invokes `process` only and does not provide streaming
`open`/`close` lifecycle. A batch writer that declares either method is rejected. A streaming writer defines
`process(row: Row) -> None`, may define `open(partition_id, epoch_id)` and `close(error)`, and must be noncallable so
PySpark dispatches to the writer protocol. The caller creates a serializable writer instance; it should open external
connections on workers rather than in the driver-side constructor.

Batch task retries and streaming epoch retries can repeat side effects. `open` and `close` are task lifecycle hooks;
`close` is not guaranteed after worker failure. Multiple queries over one transformed streaming DataFrame run
independently. The caller supplies distinct checkpoints, handles both `StreamingQuery` values, and owns progress,
failure, restart, credentials, and idempotence. Structure provides no atomicity, exactly-once, ordering, or synchronized
progress promise between the ordinary output sink and a row-level side sink. The caller can keep the existing output
query unchanged and opt into a second sink by starting it separately.
