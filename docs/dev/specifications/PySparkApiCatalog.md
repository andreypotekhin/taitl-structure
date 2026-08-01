# PySpark API Catalog Contract

## Purpose

This specification defines the checked catalog and release-ledger contract for Structure's PySpark transformation
surface. The public [API Catalog](../../APICatalog.md) is the user-facing status view; the machine-readable inventories
under `src/structure/plugin/pyspark/resources/` are the checked implementation source. This document records how those
views stay aligned and how a feature earns a support claim.

## Coverage Baseline

The catalog covers the PySpark 3.5.x/4.0.x transformation intersection. It excludes readers, writers, storage,
catalog management, actions, driver materialization, streaming lifecycle, arbitrary callbacks, RDD/Pandas APIs, and raw
SQL escape hatches. An API available only on one target profile carries an explicit capability profile and actionable
diagnostic.

Each selected family has exactly one classification in the machine-readable inventory. The catalog records the PySpark
API and aliases, Structure spelling, status, target profiles and variants, input/result types, nullability, cardinality,
semantic differences, exclusion rationale, public documentation, and evidence links.

The status vocabulary is:

- `supported` or `implemented`: the typed contract and required evidence are complete;
- `scheduled`: the feature is assigned to an approved delivery slice;
- `deferred`: the type, cardinality, determinism, or runtime contract is incomplete;
- `design-gated`: the contract is defined but implementation and evidence are still required;
- `unsupported`: Structure intentionally keeps the behavior at an explicit hook or caller-owned boundary;
- `streaming-ineligible`: the batch operation requires a materialization boundary for streaming input.

The public catalog should not leave an open row as generic `planned` or `deferred`; split mixed rows into precise
implemented, design-gated, unsupported, or streaming-ineligible portions.

## Admission Contract

A candidate becomes supported only when its source syntax is typed and small, symbolic execution determines operands,
result type, honest nullability, and cardinality, the IR records the operation without backend objects, and shared
PySpark recipes drive both online and generated execution. Target capability checks must reject unsupported versions or
variants before runtime. Diagnostics must name legal types and a compiler-visible alternative when the operation is
rejected. Unit, generated-source, parity, capability, compatibility, streaming-classification, and public-reference
evidence are required at the appropriate level.

An unsupported or deferred API must name its safe boundary: an ordinary step, explicit scalar UDF, `@raw`, or
caller-owned PySpark. No API becomes supported merely because generated code can call a PySpark function.

## Release Ledger

Every detailed ledger row contains:

```text
capability id; status; public spelling; source PySpark API; operand and result schemas;
null/empty/duplicate/order semantics; cardinality; batch/streaming/Connect support;
diagnostic code; source/recipe/evaluator/renderer owner; example client; public docs;
source, generated, parity, capability, and live test evidence; Gaps.md link when deferred.
```

The v6 raw-hook inventory remains a checked companion at
`docs/dev/specifications/ExampleRawHookInventory.json`. It records every `@raw` method under `examples/`, including
retired hooks, scheduled/deferred migrations, and intentional driver-side algorithms. A hook disposition must name a
real owner, capability or boundary, and rationale.

## Executable Evidence Matrix

Each capability is complete only when this matrix names its behavior, specification owner, and checked tests for source
capture, generated rendering, online execution, traceability, compatibility, or live behavior as applicable. Live Spark
tests may be skipped without a live target, but a skipped lane is not evidence that the target passed.

| Capability | Specification owner | Executable evidence |
| --- | --- | --- |
| Raw-hook inventory and gaps register | this document | `tests/specifications/v6-api-ledger/test_v6_raw_hook_inventory.py` |
| Security/Search migration prerequisites | this document | `tests/specifications/v6-api-ledger/test_v6_example_migration_prerequisites.py` |
| Partitioned `window_max` | `AdvancedAnalyticalOperations.md` | `tests/specifications/v6-api-ledger/test_v6_window_max_partitioning.py` |
| Ordered `collect_list` | `AdvancedAnalyticalOperations.md` | `tests/specifications/v6-api-ledger/test_v6_ordered_collect_list.py` |
| Typed relation operations | `TypedRelationOperations.md` | `tests/specifications/v6-api-ledger/test_v6_posexplode_struct.py`, `tests/specifications/v6-api-ledger/test_v6_relation_union.py`, `tests/specifications/v6-api-ledger/test_v6_relation_assertions.py` |
| Bounded ordered `scan(...)` | `OrderedTimelineScan.md` | `tests/specifications/symbolic-execution/test_ordered_timeline_scan.py`, `tests/integration/pyspark/v6/test_ordered_timeline_scan.py` |
| PySpark catalog consistency | this document | `tests/specifications/compatibility/test_pyspark_transformation_coverage.py`, `tests/specifications/backend-capabilities/test_backend_capabilities.py` |

Before a capability is marked implemented in the public catalog or `docs/dev/Gaps.md`, add its evidence row and update
the catalog, gap register, and checked coverage source in the same change.

## Streaming and Deferred Families

Streaming API families use the same source/recipe/evidence discipline, while the caller-owned lifecycle boundary is
explicit. The streaming ledger classifies each family as `structure-supported`, `caller-owned-guided`, `design-gated`,
`streaming-ineligible`, or `out-of-scope`. Lifecycle APIs are never counted as Structure transformation support.

V7 delivery slices retained this contract for typed array-of-struct generators, Binary encoding, schema-carrying JSON
and CSV conversion, deterministic grouped `mode(...)`, stream-static enrichment, and one-stateful-plus-stateless
composition. Generator variants have explicit schema, null/empty, cardinality, and streaming rules. Binary and parsing
helpers have typed options and cross-target nullability rules. Deterministic mode has explicit tie semantics. Streaming
claims require caller-owned source/sink/query examples and PySpark 3.5/4.0 evidence.

Non-streaming design-gated details—XML, Variant and optional-provider geospatial types, join reordering, nearest as-of
tie policy, aggregate alias closure, sampling, and missing-column set composition—are recorded in the public API
catalog and their domain specifications. The catalog must link each row to the precise design or specification rather
than retaining a vague backlog label.

## Acceptance

The catalog contract is complete when every selected transformation family has one checked classification, every
supported row has source syntax, IR/lowering, capability, diagnostics, docs, and test evidence, every rejected row has a
public rationale and corrective boundary, and the executable matrix and raw-hook inventory tests pass with `make build`.
