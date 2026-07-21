from examples.math.schemas.vectors import VectorEvent, VectorResult
from structure import StreamingMode, Transform, input, output, transform
from structure.plugin.pyspark import *


@transform(streaming_compatible=True)
class EvaluateVectors(Transform):
    events = input(VectorEvent, streaming=StreamingMode.YES)
    results = output(VectorResult)

    def evaluate(self, x: VectorEvent) -> VectorResult:
        same_size = size(x.x) == size(x.y)
        products = arr_zip_with(x.x, x.y, lambda x, y: x * y)
        dot = arr_aggregate(products, 0.0, lambda total, value: total + value)
        squared_x = arr_aggregate(x.x, 0.0, lambda total, value: total + value * value)
        squared_y = arr_aggregate(x.y, 0.0, lambda total, value: total + value * value)
        magnitude_x = sqrt(squared_x)
        magnitude_y = sqrt(squared_y)
        non_zero_x = magnitude_x > 0
        non_zero_y = magnitude_y > 0
        valid = same_size & non_zero_x & non_zero_y
        return VectorResult(
            vector_id=x.vector_id,
            sum=when(same_size, arr_zip_with(x.x, x.y, lambda x, y: x + y)).otherwise(None),
            difference=when(same_size, arr_zip_with(x.x, x.y, lambda x, y: x - y)).otherwise(None),
            scaled=arr_transform(x.x, lambda item: item * x.scalar),
            normalized=when(non_zero_x, arr_transform(x.x, lambda item: item / magnitude_x)).otherwise(None),
            projection=when(
                same_size & non_zero_y,
                arr_transform(x.y, lambda y: y * dot / squared_y),
            ).otherwise(None),
            dot=when(same_size, dot).otherwise(None),
            magnitude_x=magnitude_x,
            magnitude_y=magnitude_y,
            cosine=when(valid, dot / (magnitude_x * magnitude_y)).otherwise(None),
            error=when(same_size, when(non_zero_x & non_zero_y, None).otherwise("zero vector")).otherwise(
                "vector size mismatch"
            ),
        )
