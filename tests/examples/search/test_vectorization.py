from typing import cast

from examples.search.transforms.inference import Inference
from examples.search.transforms.offline.vectorization import OfflineVectorization
from examples.search.transforms.online.filtering import SelectGapQueries as SelectFilterGaps
from examples.search.transforms.online.vectorization import OnlineVectorization
from examples.search.transforms.searching.search_docs.SearchDocuments import SearchDocuments
from examples.search.transforms.vectorization import Vectorization
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model import TransformPlan


def test_inference_and_vectorization_facets_compile() -> None:
    for transform in (Inference, Vectorization, OfflineVectorization, OnlineVectorization):
        Compiler.frontend.compile()(transform, materialize_schemas=False)


def test_vectorization_facets_select_the_expected_execution_mode() -> None:
    assert OfflineVectorization.vectorized.streaming_mode is False
    assert OnlineVectorization.vectorized.streaming_mode is True


def test_search_documents_accepts_optional_extra_filter_targets() -> None:
    plan = cast(TransformPlan, Compiler.frontend.compile()(SearchDocuments, materialize_schemas=False).analysis)
    target = next(input for input in plan.inputs if input.name == "document_filter_targets")
    assert target.optional is True


def test_online_filter_gap_selection_accepts_optional_extra_filter_targets() -> None:
    plan = cast(TransformPlan, Compiler.frontend.compile()(SelectFilterGaps, materialize_schemas=False).analysis)
    target = next(input for input in plan.inputs if input.name == "document_filter_targets")
    assert target.optional is True
