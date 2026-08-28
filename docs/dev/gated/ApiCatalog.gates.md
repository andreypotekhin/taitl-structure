# API Catalog Gates

This document is the single register for API design gates. Structure admits a PySpark feature only when its typed
contract, cardinality and nullability rules, target capability, diagnostics, generated form, online behavior, and
evidence agree. Use the [Function Gates](Functions.gates.md) for function-specific gates and the
[Parity register](../Parity.md) for detailed family coverage.

## Status

- `design-gated`: a candidate has a written contract direction, but Structure does not support it yet.
- `target-gated`: the contract depends on a target or provider profile that is not part of the supported baseline.
- `streaming-ineligible`: the batch contract may be admissible, but Structure does not claim a streaming form.
- `caller-owned-guided`: Structure documents a runnable native-PySpark boundary without compiling or owning the API.
- `unsupported`: the capability is intentionally outside Structure's compiler-visible contract.

The public catalog must use a precise status and a corrective remedy. It must not leave an open row as generic
`planned` or `deferred`.

## Outstanding Gates

### Variant Mutation Profiles — `design-gated`

Append, insert, set, and delete helpers require a released target profile, typed paths and results, capability rules,
and online/generated evidence. The released Variant slice remains supported within its profile; later-profile mutation
helpers stay target-gated until the profile and contract are released.

### Streaming Missing-Column Union — `design-gated`

Batch missing-column union has typed nullable/defaulted behavior. Streaming schema evolution still needs explicit
cardinality, nullability, nested-field, alias, state, and PySpark 3.5/4.0 evidence. Use exact-schema streaming unions
or materialize to batch. The detailed implementation work is in the
[V10 API plan](../planning/P08022601.V10-api-catalog-and-schema-evolution.plan.md).

The typed API intentionally covers useful families rather than every PySpark spelling. Open function families remain in
the [Parity register](../Parity.md) and [Function Gates](Functions.gates.md) until each has a type, nullability,
determinism, cardinality, capability, and evidence decision.

The XML, geospatial, join-reordering, and directional as-of items intentionally deferred after review are recorded in
[API Catalog Deferred Work](../deferred/ApiCatalog.deferred.md).

## Admission Evidence

Before a gate moves to `implemented` or `supported`, update the public reference, capability or unsupported
diagnostic, symbolic/IR tests, generated rendering tests, online execution tests, Spark Connect evidence where claimed,
streaming classification, and the [API Catalog](../../APICatalog.md). Unavailable live evidence remains unavailable; it
is never promoted to support.

## Evidence Gates

These environment or target constraints block a stronger support claim without changing the design boundary:

- The ordinary PySpark 3.5/4.0 Docker lanes now have live evidence for the current integration selection, but the six
  shared generated-result failures remain open; the focused Spark Connect 3.5/4.0 slices pass only for the selected
  boundary/parity cases and do not clear full Search evidence.
- Optional Geometry provider evidence is positive for the pinned Sedona WKT round-trip test in the Docker lanes, but the
  provider is not bundled with the PySpark plugin and the broader provider surface remains target-gated.
- `is_valid_variant(...)` has released-profile capability evidence but no positive PySpark 4.2 live lane in this
  workspace.
- Exact Search vector retrieval and the Search generated/online comparison still need a live target lane.

Record unavailable evidence as unavailable; never promote it to `implemented` or `supported`. The current evidence
matrix is maintained in [V10 Release Evidence](../project-management/V10ReleaseEvidence.md).

## Related Records

- [Streaming gates](Streaming.gates.md) owns streaming-specific state, lifecycle, and side-effect gates.
- [API Catalog deferred work](../deferred/ApiCatalog.deferred.md) owns postponed API direction and adoption scope.
- [API Catalog](../../APICatalog.md) is the public status table.
- [Compatibility](../../Compatibility.md) defines the default PySpark target range.
