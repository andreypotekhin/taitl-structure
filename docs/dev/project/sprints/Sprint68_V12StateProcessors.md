# Sprint 68: V12 Ordinary State Processor Completion

Status: planned. Iteration 8 of 12; two weeks, with calendar dates unset.

## Sprint goal

Admit existing state processor families on ordinary 4.2 and close the legacy Pandas evidence gaps.

## User-facing outcome

Users can choose the row, Pandas processor, or legacy Pandas interface with explicit mode, timer, state, and
output-cardinality guarantees.

## Dependencies

Requires Sprint 61. Supplies profile-specific processor evidence for Sprint 69.

## Implementation tasks

- [ ] Keep row transformWithState, transformWithStateInPandas, and legacy applyInPandasWithState as separate admissions.
- [ ] Carry typed Value/List/Map states, keys, TTL, timers, and initial state onto ordinary 4.2 where native APIs
      permit.
- [ ] Finish legacy processing/event-time timeout and zero/multiple-output evidence on retained profiles and 4.2.
- [ ] Exercise late data, state expiry, timer callbacks, worker serialization, and same-configuration checkpoint
      restart.
- [ ] Record checkpoint-sensitive changes and profile remedies separately from Python source migration guidance.
- [ ] Specify any further typed processor extension before coding; retain the opaque native-processor path.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [V11 parity](../../specifications/V11PySpark41Parity.spec.md).
- [Retained gates](../../specifications/V11RetainedV9DesignGates.spec.md).
- [Streaming design gates](../../specifications/StreamingDesignGatedFeatures.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- The full configured integration matrix passes at sprint close, with evidence recorded under the
  [validation cadence](../../Testing.md#validation-cadence).
- Each admitted family/profile has its own output/time-mode, timeout/timer, cardinality, and restart results.
- Typed state kinds, composite keys, TTL expiry, and initial-state behavior match native processor behavior.
- Legacy gates close only where all missing cases pass; unsupported modes fail before query start.

## Demo and validation

Run a typed accumulator with state expiry and a legacy timeout callback that emits zero or multiple rows.
Restart the unchanged queries from caller-managed checkpoints and compare native/online/generated output.

During coding tasks, run `make build` and choose focused live checks as needed. At sprint close, run the full
configured matrix against the closing candidate, including required isolated provider phases:

```bash
make build INTEGRATION=1 BACKEND=all
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

Processing-time tests can be flaky and state changes can invalidate checkpoints.
Use isolated fixtures with progress conditions and record exact restart configuration.

## Non-goals

No Connect streaming, guaranteed close callbacks, automatic state migration, Dataset state APIs, or cross-version
restart claim.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
