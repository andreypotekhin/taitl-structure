# Streams app future

This document records competition and event-processing capabilities that could sensibly be admitted to the Streams
example later. It is a design backlog, not a promise that every item will be implemented. A future capability must define
event-time, duplicate, late-data, correction, state-retention, and output semantics before it is admitted.

The current Streams application prepares watermarked and deduplicated gate passages, builds live gate progress, and
correlates independently streamed judge calls within a bounded five-minute interval. Callers own stream sources, sinks,
checkpoints, triggers, output modes, query lifecycle, deployment, and recovery. The items below are not currently
admitted.

## Race state and results

### Race completion and winner calculation

The current example deliberately stops at gate progress and penalty correlation. A future `BuildRaceResults` workflow
could identify completed runs, calculate adjusted elapsed time, rank paddlers, and publish winners. It must define what
constitutes a start and finish, how missing gates are classified, how penalties affect elapsed time, and how ties are
represented. A live provisional result and a final batch result should not be conflated.

### Run lifecycle

A future lifecycle model could represent scheduled, started, active, finished, abandoned, disqualified, and corrected
runs. This needs explicit run-status events and correction identity. Inferring lifecycle from the last passage would make
late or missing events indistinguishable from a genuine finish.

### Split times and comparative progress

The example could publish paddler splits, pace against a target, and live comparison with a selected reference run. These
outputs need a stable comparison policy, event-time alignment, and behavior for gates not yet observed. A reference run
must be caller-selected or explicitly derived; it must not be chosen accidentally from physical arrival order.

## Competition analytics

### Course and season standings

A future batch workflow could aggregate completed races into athlete, team, club, course, and season standings. It should
handle changing course layouts, race classes, penalties, ties, withdrawals, and missing results. Season points and tie
breakers must be caller-declared policy inputs rather than hidden constants.

### Performance analysis

Streams could derive gate-to-gate improvement, consistency, penalty-adjusted pace, course-section difficulty, and
athlete-versus-baseline summaries. These are descriptive measures and need enough completed runs to avoid presenting
unstable warm-up values as conclusions.

### Anomaly and safety detection

A future safety branch could detect impossible elapsed times, reversed gate order, duplicate passage sequences, missing
mandatory gates, sudden timing drift, or paddler-location conflicts. It should publish evidence and severity, not silently
repair the event stream. Safety actions, race stoppage, and emergency communication remain caller-owned.

## Richer event sources

### Finish and timing corrections

The current raw event is a timing message. Future finish-line events, manual corrections, photo-finish decisions, and
device diagnostics could make the example more realistic. Every correction needs a stable replacement or supersession key,
an authority, and an audit timestamp so reprocessing is deterministic.

### GPS and telemetry

GPS points, speed, heading, water conditions, and equipment telemetry could support richer race analysis. This would
require spatial and temporal sampling contracts, privacy decisions, out-of-order handling, and potentially much larger
state. The first telemetry slice should remain batch-oriented unless a clear streaming state model is available.

### Officials and course conditions

Course changes, gate closures, water-level events, weather, and official decisions could be joined to passages and race
results. Effective-time versus recorded-time semantics must be explicit, especially when an official correction arrives
after a race has been displayed as complete.

## Stateful streaming capabilities

### Stateful race aggregation

Live winner boards, per-run state, timeout handling, and correction-aware retractions would exercise richer Structured
Streaming behavior than the current append-oriented passage path. Admission requires declared state keys, watermark and
timeout policy, update or complete output expectations, restart behavior, and how previously emitted provisional results
are corrected.

### Stream-stream competition joins

The current judge correlation is a bounded stream-stream join. Future joins could correlate multiple telemetry streams,
official corrections, and finish events. Each join needs event-time bounds, watermark relationships, state-retention
limits, duplicate policy, and diagnostics for unsupported output modes.

### Replay and recovery verification

A future example could include deterministic replay of a finite event log and compare a recovered query with a clean run.
That should remain an adoption and test harness around caller-owned lifecycle code, not a reason for Structure transforms
to start managing checkpoints or query recovery.

## Broader sports domain

### Other race formats

Kayaking could be extended to heats, time trials, relays, team scoring, and multi-round competitions. These should be
admitted only when their event and scoring contracts remain understandable; a generic sports engine would dilute the
example.

### Cross-race athlete profiles

A future profile workflow could combine athlete history, equipment, course, weather, and penalty patterns. It would need
identity, retention, and consent decisions, and must distinguish observed history from predictive assessment.

## Permanent boundaries

Streams does not own the following unless a separate product decision changes the architecture:

- `readStream`, `writeStream`, triggers, checkpoints, query start/stop, deployment, or recovery;
- race timing hardware, GPS ingestion, official timing systems, or emergency systems;
- side-effecting sinks, `foreach`, `foreachBatch`, or external notifications;
- authoritative race adjudication, disqualification, or safety decisions; and
- interactive dashboards or live display state.

## Admission guidance

Admit Streams features one stateful family at a time. Every addition should include a finite event fixture, out-of-order
and duplicate cases, late-data behavior, output-mode classification, generated-code evidence, and live Spark evidence when
the feature claims streaming support. Keep the lifecycle recipe under `examples/streams/adoption.py` caller-owned and
make generated transforms remain pure DataFrame transformations.

## References

- Current Streams application: `examples/streams/Readme.md`
- Streaming future boundary: `docs/dev/deferred/Streaming.deferred.md`
- Current streaming architecture: `docs/background/Streaming.back.md`
