from examples.search.schemas.indexing.vector import (
    DocumentVectorEmbedding,
    DocumentVectorIndexSummary,
    VectorIndexPolicy,
)
from examples.search.transforms.indexing.vector import ScoreVectors, VectorIndex
from structure.core.compiler.api import Compiler


def test_vector_contract_declares_non_null_double_arrays_and_policy_fields() -> None:
    vector = DocumentVectorEmbedding._structure_fields["vector"]
    assert vector.nullable is False
    assert vector.type.name == "array"
    assert vector.type.element.name == "double"
    assert vector.type.contains_null is False
    assert VectorIndexPolicy._structure_fields["rrf_k"].nullable is False
    assert DocumentVectorIndexSummary._structure_fields["target_count"].type.name == "long"


def test_vector_index_and_scoring_transforms_compile() -> None:
    Compiler.frontend.compile()(VectorIndex, materialize_schemas=False)
    Compiler.frontend.compile()(ScoreVectors, materialize_schemas=False)
