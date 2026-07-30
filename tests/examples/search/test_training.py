import pytest

from examples.search.algorithms.training import (
    GradeRegressionRanker,
    LinearArtifactScorer,
    LinearTrainingSettings,
    PairwiseLinearRanker,
    RankerCatalog,
    RankingMetrics,
    TrainingExample,
    TrainingPipeline,
    TrainingSplit,
    training_examples,
)


def _rows() -> list[TrainingExample]:
    return [
        TrainingExample("q", "a", 3.0, {"lexical_score": 2.0, "length": 2.0}),
        TrainingExample("q", "b", 0.0, {"lexical_score": 0.0, "length": 8.0}),
        TrainingExample("q", "c", 1.0, {"lexical_score": 1.0, "length": 4.0}),
    ]


def _metrics() -> RankingMetrics:
    return RankingMetrics(0.5, 0.5, 0.5, 1, 1)


def test_rankers_are_deterministic_and_interchangeable_through_catalog() -> None:
    settings = LinearTrainingSettings(epochs=100, learning_rate=0.1, l2=0.0)
    regression = GradeRegressionRanker(settings)
    pairwise = PairwiseLinearRanker(settings)
    catalog = RankerCatalog((regression, pairwise))

    regression_artifact = regression.fit(_rows(), snapshot_id="snapshot", split_seed="seed", validation=_metrics())
    pairwise_artifact = pairwise.fit(
        list(reversed(_rows())), snapshot_id="snapshot", split_seed="seed", validation=_metrics()
    )

    assert regression_artifact == regression.fit(
        _rows(), snapshot_id="snapshot", split_seed="seed", validation=_metrics()
    )
    assert catalog.scorer(regression_artifact).score(_rows()[0].features) > catalog.scorer(regression_artifact).score(
        _rows()[1].features
    )
    assert LinearArtifactScorer(pairwise_artifact).score(_rows()[0].features) > LinearArtifactScorer(
        pairwise_artifact
    ).score(_rows()[1].features)


def test_catalog_rejects_duplicate_or_incompatible_rankers() -> None:
    ranker = GradeRegressionRanker()
    catalog = RankerCatalog((ranker,))
    with pytest.raises(ValueError, match="already registered"):
        catalog.register(ranker)
    with pytest.raises(ValueError, match="No ranker"):
        catalog.trainer("missing", 1)


def test_artifact_scorer_requires_every_declared_feature() -> None:
    artifact = GradeRegressionRanker().fit(_rows(), snapshot_id="snapshot", split_seed="seed", validation=_metrics())
    with pytest.raises(ValueError, match="requires feature"):
        LinearArtifactScorer(artifact).score({"lexical_score": 1.0})


def test_snapshot_adapter_validates_grades_and_duplicate_judgments() -> None:
    row = {
        "search_query_id": "q",
        "document_id": "d",
        "relevance_grade": 3,
        "lexical_score": 1.0,
        "query_token_count": 2,
        "query_distinct_token_count": 2,
        "document_content_length": 30,
        "document_url_is_https": None,
    }
    assert training_examples([row])[0].features["document_url_is_https"] == 0.0
    with pytest.raises(ValueError, match="duplicates"):
        training_examples([row, row])
    with pytest.raises(ValueError, match="0, 1, 2, or 3"):
        training_examples([{**row, "relevance_grade": 4}])


def test_split_keeps_complete_queries_together() -> None:
    rows = _rows() + [TrainingExample("third", "d", 1.0, {"lexical_score": 1.0, "length": 1.0})]
    train, validation = TrainingSplit(seed="fixed", validation_percent=50).partition(rows)
    train_queries = {row.query_id for row in train}
    validation_queries = {row.query_id for row in validation}
    assert not train_queries & validation_queries
    assert train_queries | validation_queries == {"q", "third"}


def test_pipeline_trains_builtins_and_recommends_one_artifact() -> None:
    rows = [
        TrainingExample(query, document, relevance, {"lexical_score": score, "length": length})
        for query in ("q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7")
        for document, relevance, score, length in (("good", 3.0, 2.0, 1.0), ("bad", 0.0, 0.0, 9.0))
    ]

    run = TrainingPipeline(split=TrainingSplit(seed="pipeline", validation_percent=30)).run(
        rows, snapshot_id="snapshot"
    )

    assert {candidate.artifact.ranker_id for candidate in run.candidates} == {"grade-regression", "pairwise-linear"}
    assert run.recommended in run.candidates
    assert run.recommended.artifact.validation.query_count > 0
