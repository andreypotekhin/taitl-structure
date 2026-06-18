# Nice To Have

This document tracks useful ideas that are intentionally outside the v1, v2, v3, and v4 roadmap.

## Beyond v4

### Runtime lineage emitter

Runtime lineage emission can record transform-run facts as LDJSON after generated transforms execute. Useful facts may
include transform name, run identifier, start and end time, inputs, output, row counts when available, hook execution
markers, and runtime environment metadata.

This is deliberately deferred beyond v4. Structure is currently a compiler-first project, so v1 lineage should focus
on compiler provenance and static dataflow inferred from IR. Runtime LDJSON can be revisited after the compiler model,
generated-code contract, streaming orchestration, and Spark Connect work are stable.
