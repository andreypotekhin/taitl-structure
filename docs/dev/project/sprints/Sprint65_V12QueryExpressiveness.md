# Sprint 65: V12 Query Expressiveness

Status: planned. Iteration 5 of 12; two weeks, with calendar dates unset.

## Sprint goal

Extend V11 single-relation queries with query-local joins, grouped results, and scalar subqueries.

## User-facing outcome

Users can express analytical subqueries inside typed transforms while retaining visible schemas and correlation.

## Dependencies

Requires Sprint 61 and the V11 query model. Supplies the query infrastructure used by Sprint 66.

## Implementation tasks

- [ ] Specify relation occurrence identity, alias scope, correlation, scalar cardinality, and null/empty-result
      semantics.
- [ ] Add query-local joins and grouped/aggregate results without passing arbitrary DataFrame callbacks.
- [ ] Add a typed scalar-subquery consumer with a defined zero/one/multiple-row result policy.
- [ ] Extend EXISTS, IN, and lateral consumers to the admitted richer query descriptors with explicit result schemas.
- [ ] Classify nested queries, correlated IN, and query-local windows individually; do not promise arbitrary nesting.
- [ ] Preserve post-limit lateral predicates and current EXISTS/IN null behavior; measure compile and generated-plan
      growth.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [V11 query parity](../../specifications/V11PySpark41Parity.spec.md).
- [DSL](../../specifications/DSL.spec.md).
- [Implicit global aggregation](../../specifications/ImplicitGlobalAggregation.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- The full configured integration matrix passes at sprint close, with evidence recorded under the
  [validation cadence](../../Testing.md#validation-cadence).
- Joined, grouped, and scalar queries match native results online and generated on claimed 4.1/4.2 batch variants.
- Self-aliases, empty results, null comparisons, duplicate matches, and scalar over-cardinality have explicit tests.
- Unsupported correlation/nesting receives a source-located diagnostic; existing V11 query fixtures retain their
  behavior.

## Demo and validation

Compute a per-customer aggregate subquery and a joined correlated existence condition, then project a scalar
result. Compare native, online, and generated rows and demonstrate the scalar-cardinality failure case.

During coding tasks, run `make build` and choose focused live checks as needed. At sprint close, run the full
configured matrix against the closing candidate, including required isolated provider phases:

```bash
make build INTEGRATION=1 BACKEND=all
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

Scope capture and alias collisions can generate valid-looking but incorrect SQL plans.
Validate relation identity and preserve operation ordering through composition.

## Non-goals

No unrestricted SQL, raw DataFrame callbacks, arbitrary nested correlation, query actions, or streaming subqueries.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
