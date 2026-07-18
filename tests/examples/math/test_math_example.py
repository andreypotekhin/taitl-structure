from examples.math.transforms.algebra import EvaluateAlgebra
from examples.math.transforms.matrices import InvertMatrices, MultiplyMatrices, MultiplyMatrixVector
from examples.math.transforms.vectors import EvaluateVectors
from structure import compile_transform


def test_math_example_transforms_compile() -> None:
    for transform in (EvaluateAlgebra, EvaluateVectors, MultiplyMatrices, MultiplyMatrixVector, InvertMatrices):
        compile_transform(transform)
