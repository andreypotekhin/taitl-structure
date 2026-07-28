# School Example

This example applies Structure to simple math operations: algebra, vector and matrix operations. Structure expresses the transformations; callers own
data sources, stream lifecycle, checkpointing, and the choice to persist or display results.

| Concern | Transform | Result | Details |
| --- | --- | --- | --- |
| Scalar algebra | `EvaluateAlgebra` | Formula result per event | Streaming-compatible with static parameters. |
| Vector algebra | `EvaluateVectors` | Vector result per event | Streaming-compatible Spark array expressions. |
| Matrix product | `MultiplyMatrices` | Matrix cells | Batch join and grouped sum. |
| Matrix-vector product | `MultiplyMatrixVector` | Vector cells | Batch join and grouped sum. |
| Matrix inversion | `InvertMatrices` | Inverse matrix cells | Explicit, small-matrix driver-side batch hook. |
| Sequences | `Fibonacci`, `PrimeNumbers` | One number per ordered row | Batch PySpark recurrence with `scan(...)`. |
| Series | `PiAsSeries`, `EAsSeries`, `Ln2AsSeries` | Partial sums | Batch PySpark recurrence for finite numerical approximations. |

## Scalar Algebra

`EvaluateAlgebra` joins each `ScalarEvent` to static `FormulaParameters` by `params_id`, then calculates basic
arithmetic, linear and quadratic formulas, Euclidean distance and compound growth. It is streaming-compatible:
events may be a streaming DataFrame, while parameters are a static reference DataFrame.

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

Division by zero produces a null quotient and `"division by zero"` error. Compound growth produces a null result and
`"invalid compound parameters"` when the compounding count is non-positive or its base is negative. Those invalid
conditions are explicit data rather than failed stream queries. 

## Vector Algebra

`EvaluateVectors` accepts a streaming `VectorEvent` and returns element-wise sum and difference, scalar scaling,
normalization, projection onto the second vector, dot product, magnitudes, and cosine similarity. The implementation
uses compiler-visible Spark higher-order array expressions rather than Python UDFs.

```python
vectors = spark.readStream.schema(VECTOR_EVENT_SCHEMA).json(vector_events_path)

results = EvaluateVectors(events=vectors).run(session).results

query = (
    results.writeStream.outputMode("append")
    .option("checkpointLocation", vector_checkpoint)
    .format("memory")
    .start()
)
```

Both input vectors must have the same length for pairwise operations. A size mismatch produces null pairwise results
and `"vector size mismatch"`; a zero vector produces null normalization, cosine, and projection where appropriate,
with `"zero vector"`. Scaling and magnitude remain meaningful for a zero or unmatched vector and are still emitted. 

## Matrix Operations

`MultiplyMatrices` joins left and right cells on the shared inner
dimension, then groups and sums products. `MultiplyMatrixVector` does same for a matrix and vector.

```python
product = MultiplyMatrices(left=left_cells, right=right_cells).run(session).product
vector = MultiplyMatrixVector(matrix=left_cells, vector=vector_cells).run(session).product

product_cells = product.orderBy("matrix_id", "i", "j")
vector_cells = vector.orderBy("matrix_id", "i")
```

Matrix multiplication stays optimizer-visible as a join and aggregate - it is not routed
through less-optimal alternatives such as RDD or UDF.

### Matrix inversion

`InvertMatrices` is an raw hook. It collects each matrix on the driver and uses pivoted
Gauss-Jordan elimination. It works only with complete, square, non-singular matrices. 

```python
inverse = InvertMatrices(matrices=left_cells).run(session).inverse
inverse_cells = inverse.orderBy("matrix_id", "i", "j")
```

Inconsistent metadata,
incomplete matrices, non-square matrices, and singular matrices produce no rows. Use this only for small demonstration datasets. A distributed arbitrary-size inverse or determinant needs a dedicated
distributed pivot algorithm, which is outside of this example.

## Sequences

`Fibonacci` and `PrimeNumbers` demonstrate PySpark-plugin `scan(...)`. Rows are ordered by `index`, and each transform
emits the state before the transition for that row.

```python
from examples.school.transforms.sequences import Fibonacci, PrimeNumbers

fibonacci = Fibonacci(ticks=ticks).run(session).result.orderBy("index")
primes = PrimeNumbers(ticks=ticks).run(session).result.orderBy("index")
```

For indices `0..4`, `Fibonacci` emits `0, 1, 1, 2, 3`.
For indices `0..9`, `PrimeNumbers` emits `2, 3, 5, 7, 11, 13, 17, 19, 23, 29`.

## Series

`PiAsSeries`, `EAsSeries`, and `Ln2AsSeries` are direct transforms. Each one defines its initial state and transition
inline, so the compiled Spark plan stays visible to the optimizer.

```python
from examples.school.transforms.series import EAsSeries, Ln2AsSeries, PiAsSeries

pi = PiAsSeries(ticks=ticks).run(session).result
e = EAsSeries(ticks=ticks).run(session).result
ln2 = Ln2AsSeries(ticks=ticks).run(session).result
```

The output value is the partial sum after the current term. More input rows produce closer finite approximations.

## External Plugins

Structure allows for external plugins, allowing to apply to non-Spark environments and alternate DSLs. We include one such plugin - Iterable example plugin - under `examples/plugins/iterable/`.

Install plugin before importing the transforms:

```shell
poetry run pip install -e examples/plugins/iterable
```

`ProjectIterableScores` demonstrates the use of the external Iterable plugin and its DSL. It receives row mappings as
constructor arguments and emits named `reports` and `audits` outputs:

```python
from examples.school.transforms.iterable import ProjectIterableScores
from structure import StructureSession

scores = [{"student": "Ada", "score": 100, "ignored": False}]
profiles = [{"student": "Ada", "cohort": "math"}]
awards = [{"student": "Ada", "award": "honors"}]

result = ProjectIterableScores(students=scores, profiles=profiles, awards=awards).run(StructureSession())
assert result.reports.collect() == [
    {"student_name": "Ada", "score_points": 100, "cohort_name": "math", "award_name": "honors"}
]
```

`IterableFibonacci` is a second Iterable transform. It defines a recurrence with prior-state references. Its input contains
contiguous `index` values beginning at zero.

```python
from examples.school.transforms.iterable import IterableFibonacci

rows=({"index": index} for index in range(4))

terms = IterableFibonacci(rows=rows).run(StructureSession())
assert terms.result.collect() == [
    {"index": 0, "fibonacci_value": 0},
    {"index": 1, "fibonacci_value": 1},
    {"index": 2, "fibonacci_value": 1},
    {"index": 3, "fibonacci_value": 2},
]
```
