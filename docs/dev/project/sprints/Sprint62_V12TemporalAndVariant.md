# Sprint 62: V12 Temporal Expressions and Variant Validation

Status: planned. Iteration 2 of 12; two weeks, with calendar dates unset.

## Sprint goal

Extend V11 TIME support with useful 4.2 temporal operations and establish positive Variant-validation evidence.

## User-facing outcome

Users can convert typed TIME values to and from numeric units, bucket timestamps, and retain temporal schemas
through supported JSON/CSV conversion paths.

## Dependencies

Requires Sprint 61 runtime lanes and inventory; reuse V11 temporal and Variant behavior.

## Implementation tasks

- [ ] Specify numeric TIME conversion types, precision, range/error behavior, and timestamp bucket interval/origin
      rules.
- [ ] Implement admitted 4.2 conversions and time_bucket with independent ordinary/Connect capability checks.
- [ ] Extend declared-schema JSON/CSV conversion for TIME where the pinned runtime supports it; keep file readers
      caller-owned.
- [ ] Run positive is_valid_variant evidence and retain separate SQL-null, Variant-null, malformed-buffer, and
      extraction cases.
- [ ] Preserve V11 temporal/Variant lowering and reject unreleased mutation APIs with corrective guidance.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [V11 parity](../../specifications/V11PySpark41Parity.spec.md).
- [Schema semantics](../../specifications/SchemaSemantics.spec.md).
- [Retained Variant gates](../../specifications/V11RetainedV9DesignGates.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- The full configured integration matrix passes at sprint close, with evidence recorded under the
  [validation cadence](../../Testing.md#validation-cadence).
- Conversion and serialization cases agree natively, online, and generated for nulls and fractional precision.
- Timestamp bucketing covers supported timestamp kinds, non-default origins, session time zones, and invalid intervals.
- Variant validation has positive 4.2 evidence on each claimed variant; mutation support is not inferred.

## Demo and validation

Round-trip a declared TIME field through numeric microseconds and JSON/CSV, bucket timestamp values, and project
Variant validity. Compare native, online, and generated values and schemas on both 4.2 batch lanes.

During coding tasks, run `make build` and choose focused live checks as needed. At sprint close, run the full
configured matrix against the closing candidate, including required isolated provider phases:

```bash
make build INTEGRATION=1 BACKEND=all
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

Timestamp and TIME semantics differ; fractional conversions and session time zones can change results.
Keep literal/configuration rules explicit and test boundary values.

## Non-goals

No dynamic schema inference, file I/O ownership, generic timestamp-to-TIME casts, or unreleased Variant mutations.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
