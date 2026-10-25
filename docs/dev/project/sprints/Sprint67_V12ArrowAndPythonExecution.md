# Sprint 67: V12 Arrow and Python Execution

Status: planned. Iteration 7 of 12; two weeks, with calendar dates unset.

## Sprint goal

Extend typed Arrow aggregation with iterators and preserve Python execution behavior under 4.2 conversion changes.

## User-facing outcome

Users can process grouped Arrow batches incrementally through a declared aggregate result and receive useful
errors for schema or conversion mismatches.

## Dependencies

Requires Sprint 61 and the V11 Arrow UDF implementation. State processor callbacks remain a separate package.

## Implementation tasks

- [ ] Specify grouped aggregate iterator inputs, declared scalar results, callback lifetime, nullability, and supported
      modes.
- [ ] Extend the typed Arrow UDF path and worker serialization for admitted iterator aggregation APIs.
- [ ] Test Arrow-enabled defaults against retained explicit scalar UDF behavior and declared return-schema validation.
- [ ] Exercise nullable integer extension dtypes, large integral values, null batches, conversion errors, and empty
      inputs.
- [ ] Record ordinary/Connect batch admission independently; retain ordinary scalar/iterator streaming only with
      evidence.
- [ ] Document dependency and worker-deployment remedies without adding implicit Python execution fallback.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [UDF](../../specifications/UDF.spec.md).
- [V11 parity](../../specifications/V11PySpark41Parity.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- Grouped iterator aggregates match native Arrow results online and generated on each claimed batch variant.
- Large nullable integers retain precision; invalid returned schemas/coercions produce documented failures.
- Retained UDF modes pass their existing batch/ordinary-streaming tests, with no inferred grouped streaming admission.

## Demo and validation

Aggregate several Arrow batches into one declared result per group, including nulls and large integers.
Compare native, online, and generated results and demonstrate a return-schema mismatch.

```bash
make build
make integration BACKEND=pyspark42
make integration BACKEND=spark-connect42
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

Iterator lifetimes and conversion defaults can change memory use or value coercion.
Measure a multi-batch fixture and verify worker dependencies.

## Non-goals

No Arrow UDTF admission, implicit UDF fallback, arbitrary Pandas API coverage, or Connect/grouped-aggregate streaming.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
