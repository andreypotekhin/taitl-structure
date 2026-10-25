# Sprint 71: V12 Table Integrations and CDC

Status: planned. Iteration 11 of 12; two weeks, with calendar dates unset.

## Sprint goal

Prove 4.2 table-provider compatibility and add typed CDC consumption for supporting configurations.

## User-facing outcome

Users can keep supported Delta/Iceberg transforms on 4.2 and transform declared change rows with explicit
provider and schema requirements.

## Dependencies

Requires Sprint 61 provider feasibility and the relevant completed feature sprints; use Sprint 69 for streaming CDC.

## Implementation tasks

- [ ] Resolve and pin released Spark/client/server/provider pairs separately for ordinary and Connect batch targets.
- [ ] Run retained Delta/Iceberg reads, mutations, schema evolution, maintenance, inheritance, and composed handoffs.
- [ ] Specify typed consumption of caller-created CDC DataFrames, including operation metadata and row/schema semantics.
- [ ] Add provider-specific CDC bindings only where the catalog implements the required public changelog APIs.
- [ ] Test CDC range boundaries, inserts/updates/deletes, metadata, empty changes, and native/online/generated parity.
- [ ] Prove ordinary streaming CDC adoption with caller-created sources/checkpoints only where provider evidence
      permits.
- [ ] Document unavailable providers, unsupported capabilities, and caller remedies without claiming generic DSv2
      support.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [Delta tables](../../specifications/DeltaTables.spec.md).
- [Iceberg tables](../../specifications/IcebergTables.spec.md).
- [Streaming](../../specifications/SparkStreaming.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- Each admitted table pair passes isolated retained provider suites, including table identity and commit visibility.
- Typed change-row consumption has schema/value parity; provider-specific metadata and bounds remain explicit.
- CDC support is claimed per catalog/provider/variant; a Spark 4.2 version alone never establishes CDC availability.

## Demo and validation

Mutate a caller-provisioned table, consume its declared change relation, and compare native/online/generated rows.
Show the provider compatibility record and a corrective diagnostic for an unavailable changelog API.

```bash
make build
make integration BACKEND=pyspark42
make integration BACKEND=spark-connect42
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

A matching Spark version does not guarantee provider artifacts or catalog CDC implementation.
Use Sprint 61 findings and preserve explicit conditional admission if dependencies remain unavailable.

## Non-goals

No table/catalog creation ownership, declarative Auto CDC orchestration, cross-provider transaction guarantee, or
Connect streaming.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
