# V11 Retained V9 Design Gates Specification

## Scope

This specification defines the observable status and boundary contract for V9 items retained into V11. It does not
admit arbitrary state, row-level streaming side effects, Variant mutation, XML helpers, or join reordering.

## Normative rules

1. Each retained item has exactly one V11 status: `design-gated`, `caller-owned-guided`, `streaming-ineligible`, or
   `unsupported`.
2. A `design-gated` item must fail before lowering with a diagnostic that names the item, explains the missing contract,
   and gives the caller-owned remedy.
3. A `caller-owned-guided` item may appear only in caller-authored orchestration. Generated Structure modules must not
   contain its lifecycle or side-effect calls.
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
| Chained stateful operators | `design-gated` | Name the prior and requested state stages, then report the missing state budget, watermark, retention, output, or restart rule. |
| Row-level `foreach` | `caller-owned-guided` | Keep sink identity, retry, checkpoint, and recovery in the caller-owned streaming job. |
| Arbitrary state and `transformWithState` | `design-gated` | Reject generated state ownership and point to native Structured Streaming state APIs. |
| XML helpers | `unsupported` | Do not export or lower XML helpers. |
| Cost-based join reordering | `unsupported` | Do not export `join_order(...)` or reorder source-authored joins. |

## Acceptance evidence

The implementation is acceptable only when the catalog, machine-readable ledgers, diagnostics, generated-source scans,
and caller recipes agree with this table. Positive support for a previously gated row additionally requires a normative
contract, focused negative tests, online/generated parity, and live evidence for the claimed target and variant.
