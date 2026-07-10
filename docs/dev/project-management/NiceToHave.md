# Nice To Have

This document tracks useful ideas that are intentionally outside the v.1, v.2, v.3, and v.4 roadmap.

## Beyond v.4

### Runtime traceability emitter

Runtime traceability emission can record transform-run facts as LDJSON after online or generated transforms execute. Useful facts may
include transform name, run identifier, start and end time, inputs, output, row counts when available, hook execution
markers, and runtime environment metadata.

This is deliberately deferred beyond v.4. Structure is currently an IR-first project, so v.1 traceability should focus
on compiler provenance and static dataflow inferred from IR. Runtime LDJSON can be revisited after the compiler model,
online/generated runtime contract, streaming orchestration, and Spark Connect work are stable.
