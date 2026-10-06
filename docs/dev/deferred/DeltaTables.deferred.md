# Delta Tables Parity Queue

The [Delta design](../design/DeltaTables.design.md) and [specification](../specifications/DeltaTables.spec.md) define
the implemented transform contract. This queue lists native Delta features still outside that contract. The current
implementation plan is [V11 Delta parity and maintenance](../planning/past/P10052603.V11-delta-parity-inspection-and-maintenance.plan.md).

## Implemented and admitted surface

Typed delete, update, merge, append, selective `replaceWhere`, snapshot/CDF/history/detail reads, explicit schema
evolution, generated/identity/default field declarations, restore, optimize, and vacuum run through caller-owned table
bindings. Structure has live online/generated evidence for PySpark 3.5.3 / Delta 3.3.3, PySpark 4.0.0 / Delta 4.0.1,
and PySpark 4.1.0 / Delta 4.1.0. These are tested Structure pairs, distinct from upstream compatibility claims. The
classic ordinary profiles admit these helpers; Spark Connect and PySpark 4.2 remain outside this admission. See the
[compatibility ledger](../../compatibility/DeltaTables.compat.md) for the exact scope.

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
applicable, a focused public example, pinned live runtime evidence, and a compatibility-ledger row. This admission
covers classic PySpark 3.5–4.1 with separate pinned evidence.
