from examples.search.transforms.inference import Inference
from examples.search.transforms.offline.vectorization import OfflineVectorization
from examples.search.transforms.online.vectorization import OnlineVectorization
from examples.search.transforms.vectorization import Vectorization
from structure.core.compiler.api import Compiler


def test_inference_and_vectorization_facets_compile() -> None:
    for transform in (Inference, Vectorization, OfflineVectorization, OnlineVectorization):
        Compiler.frontend.compile()(transform, materialize_schemas=False)


def test_vectorization_facets_select_the_expected_execution_mode() -> None:
    assert OfflineVectorization.vectorized.streaming_mode is False
    assert OnlineVectorization.vectorized.streaming_mode is True
