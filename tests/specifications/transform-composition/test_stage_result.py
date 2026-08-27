import pytest

from structure import TransformResult


def test_transform_result_exposes_recursive_stage_outputs_without_flattening() -> None:
    schema = object()
    result = TransformResult(
        {"results": "ranked"},
        stage_records=[
            (("vectorized", "query_embeddings"), "queries", schema, ()),
            (("vectorized", "merged_queries", "embeddings"), "merged", schema, ()),
        ],
    )

    assert result.results == "ranked"
    assert result.vectorized.query_embeddings == "queries"
    assert result.vectorized.merged_queries.embeddings == "merged"
    assert result.stages["vectorized"]["query_embeddings"] == "queries"
    assert list(result) == ["results"]
    assert result.as_dict() == {"results": "ranked"}


def test_transform_result_can_disable_stage_output_access() -> None:
    result = TransformResult(
        {"results": "ranked"},
        stage_outputs_enabled=False,
        stage_names=("vectorized",),
    )

    assert result.results == "ranked"
    with pytest.raises(AttributeError, match="allow_stage_outputs=True"):
        _ = result.vectorized
