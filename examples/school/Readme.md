# School Example

This example shows Structure applied to ordinary mathematics data.

`transforms/algebra.py` and `transforms/vectors.py` are row-local, streaming-compatible transforms. The caller supplies
the stream and its lifecycle; Structure joins each scalar event to static formula parameters by `params_id`, then emits
formula results. Vector results use compiler-visible Spark higher-order array expressions, not Python UDFs.

```python
events = spark.readStream.schema(SCALAR_EVENT_SCHEMA).json(events_path)
results = EvaluateAlgebra(events=events, parameters=parameters).run(session).results
query = results.writeStream.outputMode("append").option("checkpointLocation", checkpoint).format("memory").start()
```

`transforms/matrices.py` is batch-only. Matrix cells use `matrix_id`, `rows`, `columns`, `i`, `j`, and `x`; matrix
multiplication is an optimizer-visible join and grouped sum. `InvertMatrices` is an explicit raw batch hook because
Structure does not yet expose iterative pivot operations as DSL expressions.

`RowMatrix` is not the implementation path: its RDD API offers SVD, QR, covariance/Gramian calculations, and
multiplication by a local dense matrix, but no inverse or determinant operation. See the
[Spark RowMatrix API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.mllib.linalg.distributed.RowMatrix.html).

## Current Matrix Boundary

The inverse hook currently materializes each matrix on the driver to perform pivoted Gauss–Jordan elimination. It is
appropriate only for small batch examples. A fully distributed arbitrary-size determinant/inverse requires a dedicated
Structure raw-output capability plus a distributed pivot algorithm; it is intentionally not claimed by this example.
