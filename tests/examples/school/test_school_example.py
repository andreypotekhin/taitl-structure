from examples.school.transforms.algebra import EvaluateAlgebra
from examples.school.transforms.matrices import InvertMatrices, MultiplyMatrices, MultiplyMatrixVector
from examples.school.transforms.vectors import EvaluateVectors
from structure.core.compiler.api import Compiler


def test_school_example_transforms_compile() -> None:
    for transform in (EvaluateAlgebra, EvaluateVectors, MultiplyMatrices, MultiplyMatrixVector, InvertMatrices):
        Compiler.frontend.compile()(transform, materialize_schemas=False)
