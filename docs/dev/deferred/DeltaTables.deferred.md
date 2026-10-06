# Delta Tables Parity Queue

The [Delta design](../design/DeltaTables.design.md) and [specification](../specifications/DeltaTables.spec.md) define
the implemented transform contract. This queue lists native Delta features still outside that contract. The current
implementation plan is [V11 Delta parity and maintenance](../planning/past/P10052603.V11-delta-parity-inspection-and-maintenance.plan.md).

## Implemented surface, awaiting broader release admission

Typed delete, update, merge, append, selective `replaceWhere`, snapshot/CDF/history/detail reads, explicit schema
evolution, generated/identity/default field declarations, restore, optimize, and vacuum run through caller-owned table
bindings. PySpark 4.1.0 / Delta 4.1.0 has focused and live integration evidence. Broader target-matrix evidence remains
pending; the compatibility ledger records the scope precisely.

## Deferred table administration

Table creation/replacement, clone, convert, native constraint installation, protocol/feature upgrades, manifest
generation, and arbitrary SQL remain caller-owned. Each needs an ownership contract before it can join the transform
surface. In particular, these calls can alter metadata or table protocol independently of row mutations.

## Deferred writer and maintenance extensions

The implemented restore/optimize/vacuum methods intentionally do not expose vendor metrics as typed transform outputs.
A multi-result effect design is needed before those metrics become Structure relations. Other optimize modes and
vendor-specific tuning options need separate parity review. Vacuum remains a destructive maintenance effect whose
retention and active-reader coordination must be considered by the caller.

`delta_generated(field, as_="...")` currently accepts a static SQL expression and checks it against Delta log
metadata. A Schema-class expression builder that provides typed field references and static expression validation is a
future ergonomics improvement.

## Streaming CDF

Streaming CDF remains a caller-created `readStream` DataFrame passed through ordinary streaming `input(Schema,
streaming=True)`. The caller owns query start, checkpoint, sink, restart, and shutdown. A future Delta-specific streaming
binding would need to specify option behavior after checkpoint recovery and Delta transaction-log coordination.

## Admission evidence

Each new family requires Spark-free compile diagnostics, generated-source inspection, online/generated parity when
applicable, a focused public example, pinned live runtime evidence, and a compatibility-ledger row. Spark Connect is
not admitted until its own Delta support and test environment are available.
