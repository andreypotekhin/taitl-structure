# Gotchas

This page is an index of reproducible operational traps. Detailed end-user guidance belongs in the linked gotcha files;
measurements, design decisions, and resolution evidence belong in the related issue records.

### Search integration is slow with tiny fixtures

Small Search fixtures can spend most of their time constructing a large Spark query plan before collecting a few rows.
See [Performance troubleshooting](troubleshooting/performance/Performance.trbl.md) and the
[Search integration slow gotcha](troubleshooting/performance/search_integration_slow.gotcha.md).

### Reused PySpark query plans exhaust driver memory

Repeated joins, unions, or other reuse of an already-expanded DataFrame can exhaust the driver even with tiny inputs.
See [Memory troubleshooting](troubleshooting/memory/Memory.trbl.md) and the
[driver-heap gotcha](troubleshooting/memory/spark_driver_heap_oom.gotcha.md).
