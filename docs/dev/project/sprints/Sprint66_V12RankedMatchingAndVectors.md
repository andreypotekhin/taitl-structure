# Sprint 66: V12 Ranked Matching and Native Vectors

Status: planned. Iteration 6 of 12; two weeks, with calendar dates unset.

## Sprint goal

Add typed native nearest-by ranking and useful 4.2 vector operations without weakening existing selection guarantees.

## User-facing outcome

Users can request up to K ranked candidates per input row and use optimizer-visible vector computations.

## Dependencies

Requires Sprints 61 and 65. Reuse existing Search/vector semantics as comparison fixtures.

## Implementation tasks

- [ ] Specify nearest-by cardinality, aliases, inner/left output schemas, ranking direction, exact/approx modes, and
      tie behavior.
- [ ] Implement typed lowering to the public nearestByJoin API; document cross-product cost and candidate filtering.
- [ ] Review available vector expressions and aggregates; choose public typed lowering per operation before
      implementation.
- [ ] Reconcile native null/NaN, zero-vector, dimensions, and numeric domains with existing Structure vector helpers.
- [ ] Classify K-element min/max overloads with the ranking package and implement admitted result shapes.
- [ ] Provide batch examples with native/online/generated results and measured candidate-count/plan behavior.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [V11 query parity](../../specifications/V11PySpark41Parity.spec.md).
- [DSL](../../specifications/DSL.spec.md).
- [Nullability and coercion](../../specifications/NullabilityAndTypeCoercion.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- Ranked output cardinality and left-side null extension match native behavior, including empty candidate pools.
- Ties and approximate modes are documented and tested without implying deterministic or index-backed results.
- Existing deterministic selection APIs retain their guarantees; vector validation and numerical tolerances are
  explicit.

## Demo and validation

Rank a filtered candidate relation for several query rows, compare all three execution paths, and show how
candidate count affects the plan. Exercise zero/mismatched vectors and tied ranking values.

```bash
make build
make integration BACKEND=pyspark42
make integration BACKEND=spark-connect42
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

Native ranking currently evaluates a cross-product, and equal scores do not establish deterministic winners.
Do not infer an ANN index from the approximate-mode option.

## Non-goals

No external ANN service, embedding/model execution, Connect streaming, or silent replacement of existing ranking
semantics.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
