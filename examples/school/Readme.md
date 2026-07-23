# School Example

This example applies Structure to simple math operations: algebra, vector and matrix operations. Structure expresses the transformations; callers own
data sources, stream lifecycle, checkpointing, and the choice to persist or display results.

| Concern | Transform | Result | Boundary |
| --- | --- | --- | --- |
| Scalar algebra | `EvaluateAlgebra` | Formula result per event | Streaming-compatible with static parameters. |
| Vector algebra | `EvaluateVectors` | Vector result per event | Streaming-compatible Spark array expressions. |
| Matrix product | `MultiplyMatrices` | Matrix cells | Batch join and grouped sum. |
| Matrix-vector product | `MultiplyMatrixVector` | Vector cells | Batch join and grouped sum. |
| Matrix inversion | `InvertMatrices` | Inverse matrix cells | Explicit, small-matrix driver-side batch hook. |

## Scalar Algebra

`EvaluateAlgebra` joins each `ScalarEvent` to static `FormulaParameters` by `params_id`, then calculates basic
arithmetic, linear and quadratic formulas, Euclidean distance and compound growth. It is streaming-compatible:
events may be a streaming DataFrame, while parameters are a static reference DataFrame.

Division by zero produces a null quotient and `"division by zero"` error. Compound growth produces a null result and
`"invalid compound parameters"` when the compounding count is non-positive or its base is negative. Those invalid
conditions are explicit data rather than failed stream queries. 

Use this streaming transform like so:

```python
events = spark.readStream.schema(SCALAR_EVENT_SCHEMA).json(events_path)
results = EvaluateAlgebra(events=events, parameters=parameters).run(session).results

query = (
    results.writeStream.outputMode("append")
    .option("checkpointLocation", checkpoint)
    .format("memory")
    .start()
)
```

## Vector Algebra

`EvaluateVectors` accepts a streaming `VectorEvent` and returns element-wise sum and difference, scalar scaling,
normalization, projection onto the second vector, dot product, magnitudes, and cosine similarity. The implementation
uses compiler-visible Spark higher-order array expressions rather than Python UDFs.

Both input vectors must have the same length for pairwise operations. A size mismatch produces null pairwise results
and `"vector size mismatch"`; a zero vector produces null normalization, cosine, and projection where appropriate,
with `"zero vector"`. Scaling and magnitude remain meaningful for a zero or unmatched vector and are still emitted. Example use:

```python
vectors = spark.readStream.schema(VECTOR_EVENT_SCHEMA).json(vector_events_path)
results = EvaluateVectors(events=vectors).run(session).results
```

## Matrix Operations

Matrix cells identify a matrix by `matrix_id` and describe its shape with `rows` and `columns`. Each cell has zero-based
coordinates `i` and `j` and a value `x`. `MultiplyMatrices` joins compatible left and right cells on the shared inner
dimension, then groups and sums products. `MultiplyMatrixVector` follows the same pattern for a matrix and vector.

```python
product = MultiplyMatrices(left=left_cells, right=right_cells).run(session).product
vector = MultiplyMatrixVector(matrix=left_cells, vector=vector_cells).run(session).product
```

These are batch transforms. Matrix multiplication stays optimizer-visible as a join and aggregate; it is not routed
through an RDD API or a Python UDF.

### Matrix inversion boundary

`InvertMatrices` is an intentionally explicit raw batch hook. It collects each matrix on the driver and uses pivoted
Gauss-Jordan elimination. It emits cells only for complete, square, non-singular matrices; inconsistent metadata,
incomplete matrices, non-square matrices, and singular matrices produce no inverse rows.

```python
inverse = InvertMatrices(matrices=left_cells).run(session).inverse
```

Use this only for small demonstration datasets. A distributed arbitrary-size inverse or determinant needs a dedicated
distributed pivot algorithm and is deliberately outside this example's claim. Spark's `RowMatrix` API is not an
alternative inverse path: it supports decompositions and selected multiplications, but not determinant or inverse
operations. Consult the Spark `RowMatrix` API reference for its supported operations.
