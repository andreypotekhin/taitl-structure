# Sprint 63: V12 Native Geospatial Expansion

Status: planned. Iteration 3 of 12; two weeks, with calendar dates unset.

## Sprint goal

Carry the V11 spatial model onto 4.2 and extend the admitted Apache native surface.

## User-facing outcome

Users can use evidenced native spatial signatures and preserve geometry/geography representation and SRID metadata.

## Dependencies

Requires Sprint 61; preserve the V11 Apache/Sedona isolation findings as the baseline.

## Implementation tasks

- [ ] Review native 4.2 public APIs, constructor SRID and binary endianness options, and supported representation
      conversions.
- [ ] Specify and implement typed extensions with explicit nullability, malformed-input, SRID, and result-kind rules.
- [ ] Verify native type materialization and schema round-trips independently from function execution.
- [ ] Retest Apache/Sedona registration collisions in isolated provider sessions; change isolation guidance only with
      evidence.
- [ ] Record managed Databricks/Connect availability separately and retain per-provider capabilities and remedies.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [Geospatial](../../specifications/Geospatial.spec.md).
- [Schema semantics](../../specifications/SchemaSemantics.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- The full configured integration matrix passes at sprint close, with evidence recorded under the
  [validation cadence](../../Testing.md#validation-cadence).
- Every selected helper and type has native/online/generated evidence on each claimed batch target.
- Tests cover fixed/mixed SRID, nulls, malformed WKB, optional arguments, and incompatible spatial representations.
- Apache evidence does not promote Sedona or managed runtimes, and mixed-session guidance agrees with live results.

## Demo and validation

Parse WKB into a declared native spatial schema, apply admitted SRID/serialization options, and compare values and
schema metadata across all three execution paths. Demonstrate an incompatible-representation diagnostic.

During coding tasks, run `make build` and choose focused live checks as needed. At sprint close, run the full
configured matrix against the closing candidate, including required isolated provider phases:

```bash
make build INTEGRATION=1 BACKEND=all
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

SQL function registrations and provider backports can differ from the Python module surface.
Use isolated sessions and exact dependency records.

## Non-goals

No implicit provider selection, silent coordinate transformation, unsafe inline bridges, or untested managed admission.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
