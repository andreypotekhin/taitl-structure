# Sprint 64: V12 Tuple Sketches and Aggregate Semantics

Status: planned. Iteration 4 of 12; two weeks, with calendar dates unset.

## Sprint goal

Extend V11 sketch types with Tuple summaries and admit selected 4.2 aggregate semantic extensions.

## User-facing outcome

Users can aggregate distinct keys with typed summaries and combine supported Tuple/Theta values without losing
logical type information.

## Dependencies

Requires Sprint 61 and the existing V11 logical sketch metadata implementation.

## Implementation tasks

- [ ] Specify Tuple key/summary domains, combination policies, null/empty behavior, precision, and Binary metadata.
- [ ] Implement selected Tuple construction, estimate/summary, and Tuple/Theta set operations with type validation.
- [ ] Carry KLL/Theta onto both 4.2 batch variants and ordinary streaming where their own evidence passes.
- [ ] Classify and implement selected collection null policies and filtered window aggregates from the 4.2 delta.
- [ ] Keep metadata/version validation and incompatible-family diagnostics consistent across schema nesting and
      persistence.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [Sketch and bitmap](../../specifications/SketchBitmap.spec.md).
- [V11 parity](../../specifications/V11PySpark41Parity.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- The full configured integration matrix passes at sprint close, with evidence recorded under the
  [validation cadence](../../Testing.md#validation-cadence).
- Tuple summaries and mixed Tuple/Theta operations match native results within declared approximation tolerances.
- Logical types survive supported schema/persistence round-trips; incompatible domains fail before Spark execution.
- Aggregate extensions cover empty/all-null groups and windows without changing neighboring metrics or grouping keys.

## Demo and validation

Build sketches for overlapping key groups, combine them, and compare estimates and summaries natively, online,
and generated. Show a rejected domain mismatch and a metric-local window filter.

During coding tasks, run `make build` and choose focused live checks as needed. At sprint close, run the full
configured matrix against the closing candidate, including required isolated provider phases:

```bash
make build INTEGRATION=1 BACKEND=all
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

Approximation tests must use justified bounds rather than exact counts. Summary combination semantics can change
results even when the underlying Binary schemas match.

## Non-goals

No untyped sketch buffers, cross-library portability guarantee, invented KLL merge APIs, or Connect streaming.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
