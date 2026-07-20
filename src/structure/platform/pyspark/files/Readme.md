# PySpark Files App

## Purpose
Compares and writes generated PySpark project files.

## Dependency Exchanges
The app consumes generated path-to-source maps from `PySpark.render.project()` and returns file-change results through
`PySpark.files`. CLI and generation callers use these endpoint commands instead of file-app internals.

## Inner Workings
`compare()` identifies added, modified, unchanged, and obsolete generated files. `write()` materializes the supplied
files and reports the same result model.
