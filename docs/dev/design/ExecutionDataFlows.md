# Design: Execution and Data Flows

## Compile Flow

```text
1. Load config.
2. Discover source modules.
3. Inspect schemas and transforms.
4. Symbolically execute subtransforms in source order.
5. Build TransformPlan IR.
6. Run compileability checks.
7. Lower checked IR to shared PySpark execution recipes.
8. Emit PySpark schema modules.
9. Emit PySpark transform classes from the shared recipes.
10. Emit runtime support.
11. Build compiler provenance.
12. Infer static dataflow traceability.
13. Format changed files.
14. Report compile metrics.
```

## Online Runtime Flow

```text
1. Caller creates SparkSession.
2. Caller creates StructureSession with spark, optional ctx, and optional config.
3. Caller constructs a transform invocation with named input DataFrames.
4. Transform.run(session) delegates to StructureSession.run(transform).
5. Session resolves execution_mode, target_backend, and target_pyspark.
6. Session selects OnlinePySparkRunner.
7. Runner compiles or retrieves TransformPlan IR.
8. Runner lowers checked IR to shared PySpark execution recipes.
9. Runner validates inputs according to recipes.
10. Runner executes DataFrame operations and hooks in recipe order.
11. Runner validates intermediates and final output according to recipes.
12. Caller writes or further composes the returned DataFrame.
```

## Runtime Batch Flow

```text
1. Airflow or job creates SparkSession.
2. Caller reads input DataFrames.
3. Caller runs a transform online through StructureSession, or imports a generated transform class when configured for
   generated mode.
4. Runtime validates inputs.
5. Runtime executes DataFrame operations.
6. Runtime validates intermediates by default.
7. Runtime calls hooks where explicit.
8. Runtime validates final output.
9. Caller writes result.
```

## Runtime Streaming-Compatible Flow

```text
1. Caller creates streaming DataFrame using Spark readStream.
2. Caller passes streaming DataFrame to an online or generated transform.
3. Runtime applies streaming-compatible DataFrame operations.
4. Caller owns writeStream, trigger, output mode, checkpoint, and lifecycle.
```

## Serial N-Join Flow

```text
Input A
  -> normalize
  -> join B
  -> join C
  -> join D
  -> ...
  -> final schema
```

The architecture does not special-case three inputs. Any number of named inputs can be declared, and source-ordered
subtransforms can use them.
