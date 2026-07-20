from examples.math.transforms.algebra import EvaluateAlgebra
from examples.math.transforms.matrices import InvertMatrices, MultiplyMatrices, MultiplyMatrixVector
from examples.math.transforms.vectors import EvaluateVectors
from structure.core.compiler.api import Compiler


def test_math_example_transforms_compile() -> None:
    for transform in (EvaluateAlgebra, EvaluateVectors, MultiplyMatrices, MultiplyMatrixVector, InvertMatrices):
        Compiler.frontend.compile()(transform, materialize_schemas=False)
