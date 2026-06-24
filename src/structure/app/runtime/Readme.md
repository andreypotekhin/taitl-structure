# Runtime App

## Purpose
The runtime app owns execution after a user has instantiated a transform with concrete inputs. It keeps online and
generated execution behind `StructureSession` so callers can choose an execution mode without learning compiler or
target internals.

## Dependency Exchanges
The app consumes a bound `Transform`, a Spark session-like object, optional caller context, execution mode, generated
package settings, compiler IR, PySpark target recipes, and runtime schema materialization. It returns DataFrame results
or `TransformResult` mappings, and raises `StructureRuntimeError` for invalid runtime wiring.

The compound `runtime` API endpoint exposes runtime commands without leaking `logic` imports:

```python
runtime.schemas.build()
runtime.execution.online.pyspark()
runtime.execution.generated.pyspark()
```

Each subcommand returns a fresh action instance. `StructureSession`, `TransformResult`, `TransformSchemas`, and runtime
diagnostic types remain available from `structure.app.runtime.api`.

## Inner Workings
Runtime is split into `session`, `schemas`, and `execution`. `StructureSession` compiles and lowers transforms, the
schemas app builds schema materializations, and execution apps either interpret PySpark recipes online or import
and run generated PySpark classes.
