# Design: Execution and Data Flows

## Compile Flow

```text
1. Load config.
2. Discover source modules.
3. Inspect schemas and transforms.
4. Symbolically execute subtransforms in source order.
5. Build TransformPlan IR.
6. Run compileability checks.
7. Emit PySpark schema modules.
8. Emit PySpark transform classes.
9. Emit runtime support.
10. Emit LDJSON lineage.
11. Format changed files.
12. Report compile metrics.
```

## Runtime Batch Flow

```text
1. Airflow or job creates SparkSession.
2. Caller reads input DataFrames.
3. Caller instantiates generated transform class.
4. Generated code validates inputs.
5. Generated code executes DataFrame operations.
6. Generated code validates intermediates by default.
7. Generated code calls hooks where explicit.
8. Generated code validates final output.
9. Caller writes result.
```

## Runtime Streaming-Compatible Flow

```text
1. Caller creates streaming DataFrame using Spark readStream.
2. Caller passes streaming DataFrame to generated transform.
3. Generated transform applies streaming-compatible DataFrame operations.
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

The architecture does not special-case three inputs. Any number of named inputs can be declared, and source-ordered subtransforms can use them.
