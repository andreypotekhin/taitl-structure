# Sprint 69: V12 Streaming Composition and Joins

Status: planned. Iteration 9 of 12; two weeks, with calendar dates unset.

## Sprint goal

Extend ordinary streaming composition with proven stateful transitions, Update-mode joins, and schema-safe unions.

## User-facing outcome

Users can combine more streaming transformations with explicit watermark, retention, schema, and output-mode rules.

## Dependencies

Requires Sprints 61 and 68. Ordinary state support alone does not admit a processor into a chain.

## Implementation tasks

- [ ] Re-admit the two existing stateful pairs on 4.1/4.2 after late-arrival, eviction, and restart checks.
- [ ] Specify additional selected stage transitions, including processor candidates, before admitting them.
- [ ] Implement supported non-outer stream-stream joins in Update mode with explicit bounds and watermark rules.
- [ ] Specify and prove missing-column streaming union for declared nullable/defaulted fields, nested structs, and
      aliases.
- [ ] Carry per-operator budgets, progress mapping, and corrective diagnostics through admitted composed stages.
- [ ] Reject reversed, third-stateful, or otherwise unproven combinations instead of inferring composition support.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [Streaming](../../specifications/SparkStreaming.spec.md).
- [Streaming compatibility](../../specifications/StreamingCompatibility.spec.md).
- [Streaming design gates](../../specifications/StreamingDesignGatedFeatures.spec.md).
- [Schema semantics](../../specifications/SchemaSemantics.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- Each newly admitted pair/profile proves watermark propagation, output mode, state retention/eviction, and restart.
- Update-mode join output agrees with native Spark for matched, late, and out-of-bound rows.
- Union preserves declared nullability, nested schemas, aliases, and row multiplicity; unsafe evolution is rejected.

## Demo and validation

Run the admitted dedupe/window pair with on-time and late events, inspect per-stage progress, and restart it.
Separately compare an Update-mode join and a missing-column streaming union across all execution paths.

```bash
make build
make integration BACKEND=pyspark42
make integration BACKEND=spark-connect42
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

A supported individual operator may be invalid in composition. Set concrete candidate scope in the sprint design;
an unproven combination remains rejected and cannot be represented as a general chaining feature.

## Non-goals

No arbitrary stateful chains, global streaming ranking, hidden query lifecycle, Connect streaming, or checkpoint
migration.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
