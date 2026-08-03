from __future__ import annotations

import difflib
from typing import cast

import pytest
from helpers.example_projects import (
    ROOT,
    expected_school_iterable_generated,
    expected_search_generated,
    expected_security_generated,
    expected_stocks_generated,
    expected_store_generated,
    expected_streams_generated,
    render_school_iterable_example,
    render_search_example,
    render_security_example,
    render_stocks_example,
    render_store_example,
    render_streams_example,
)

from structure import StructureConfig, Transform
from structure.core.cli.commands.DiscoverStructureProject import DiscoverStructureProject
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model import TransformPlan


@pytest.mark.parametrize(
    ("actual", "expected"),
    [
        (render_store_example, expected_store_generated),
        (render_streams_example, expected_streams_generated),
        (render_stocks_example, expected_stocks_generated),
        (render_security_example, expected_security_generated),
        (render_search_example, expected_search_generated),
        (render_school_iterable_example, expected_school_iterable_generated),
    ],
)
def test_example_generated_output_matches_golden_files(actual, expected) -> None:
    actual = actual()
    expected = expected()

    assert set(actual) == set(expected), _paths_diff(actual, expected)
    for path in expected:
        assert actual[path] == expected[path], _text_diff(path, expected[path], actual[path])


def test_search_scoring_subpackage_transform_is_discovered_and_compiled() -> None:
    """A nested transform module is a normal source-discovery entrypoint."""

    config = StructureConfig.resolve(project_root=ROOT, source_roots=["examples/search"])
    project = DiscoverStructureProject()(config)
    scoring = next(
        transform
        for transform in project.transforms
        if transform.__module__ == "examples.search.transforms.scoring.Scoring" and transform.__name__ == "Scoring"
    )

    Compiler.frontend.compile()(scoring, config=config, materialize_schemas=False)


def test_search_documents_propagates_streaming_query_lineage() -> None:
    """The online document-search graph declares every query-derived boundary as streaming."""

    from examples.search.transforms.scoring.ScoreBase import ScoreBase
    from examples.search.transforms.scoring.Scoring import Scoring
    from examples.search.transforms.searching.online.scoring import OnlineScoring, SelectGapQueries
    from examples.search.transforms.searching.search_docs import OverlapDocuments, RerankDocuments, RetrieveDocuments
    from examples.search.transforms.searching.search_docs.SearchDocuments import SearchDocuments

    declarations = (
        (SearchDocuments, "queries"),
        (SearchDocuments, "requests"),
        (OnlineScoring, "queries"),
        (OnlineScoring, "requests"),
        (SelectGapQueries, "queries"),
        (SelectGapQueries, "requests"),
        (Scoring, "queries"),
        (ScoreBase, "queries"),
        (RetrieveDocuments, "queries"),
        (OverlapDocuments, "candidates"),
        (RerankDocuments, "overlapped_candidates"),
    )
    for transform, input_name in declarations:
        assert getattr(transform, input_name).streaming


def test_search_query_declares_immutable_event_time() -> None:
    """Streaming query serving has a required event-time field."""

    from examples.search.schemas.search import SearchQuery

    field = SearchQuery._structure_fields["requested_at"]
    assert field.nullable is False
    assert field.type.name == "timestamp"


def test_search_all_builds_the_complete_offline_artifact_graph() -> None:
    """The build facade publishes pre-serving artifacts without result-presentation dependencies."""

    from examples.search.schemas.analytics import (
        CorpusStatistics,
        CorpusVocabulary,
        DocumentProfile,
        DocumentStatistics,
        ParagraphStatistics,
        SectionStatistics,
        SentenceStatistics,
        SimilarDocument,
    )
    from examples.search.schemas.clicks import DailyClicks, DailyImpressions
    from examples.search.schemas.indexing.lexical.index import (
        DocumentIndexSummary,
        DocumentIndexTerm,
        ParagraphIndexSummary,
        ParagraphIndexTerm,
        SectionIndexSummary,
        SectionIndexTerm,
        SentenceIndexSummary,
        SentenceIndexTerm,
    )
    from examples.search.schemas.label import Intent, IntentPattern, QueryLabel
    from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
    from examples.search.schemas.scoring.bm25 import (
        DocumentBm25Score,
        ParagraphBm25Score,
        SectionBm25Score,
        SentenceBm25Score,
    )
    from examples.search.schemas.scoring.overlap import (
        DocumentOverlapScore,
        ParagraphOverlapScore,
        SectionOverlapScore,
        SentenceOverlapScore,
    )
    from examples.search.schemas.search import (
        DocumentScore,
        ParagraphScore,
        ScorePolicy,
        SearchQuery,
        SectionScore,
        SentenceScore,
    )
    from examples.search.schemas.similarity import (
        DocumentSimilarity,
        ParagraphSimilarity,
        SectionSimilarity,
        SentenceSimilarity,
        SimilarityPolicy,
    )
    from examples.search.schemas.text import Document, Paragraph, Section, Sentence, Word
    from examples.search.schemas.user import Band, BandFallback, BandMembership, User, UserBand, UserBandMembership
    from examples.search.transforms.all import All

    plan = Compiler.frontend.compile()(All, materialize_schemas=False).analysis
    assert isinstance(plan, TransformPlan)
    assert [(item.name, item.schema) for item in plan.inputs] == [
        ("documents", Document),
        ("similarity_policy", SimilarityPolicy),
        ("score_policy", ScorePolicy),
        ("queries", SearchQuery),
        ("intents", Intent),
        ("patterns", IntentPattern),
        ("query_labels", QueryLabel),
        ("daily_impressions", DailyImpressions),
        ("users", User),
        ("bands", Band),
        ("daily_clicks", DailyClicks),
        ("policy", RelevancePolicy),
    ]
    assert [(item.name, item.schema) for item in plan.outputs] == [
        ("sections", Section),
        ("paragraphs", Paragraph),
        ("sentences", Sentence),
        ("words", Word),
        ("document_profiles", DocumentProfile),
        ("sentence_statistics", SentenceStatistics),
        ("paragraph_statistics", ParagraphStatistics),
        ("section_statistics", SectionStatistics),
        ("document_statistics", DocumentStatistics),
        ("similar_documents", SimilarDocument),
        ("corpus_statistics", CorpusStatistics),
        ("corpus_vocabulary", CorpusVocabulary),
        ("document_terms", DocumentIndexTerm),
        ("document_summary", DocumentIndexSummary),
        ("section_terms", SectionIndexTerm),
        ("section_summary", SectionIndexSummary),
        ("paragraph_terms", ParagraphIndexTerm),
        ("paragraph_summary", ParagraphIndexSummary),
        ("sentence_terms", SentenceIndexTerm),
        ("sentence_summary", SentenceIndexSummary),
        ("labeled_queries", SearchQuery),
        ("document_scores", DocumentScore),
        ("section_scores", SectionScore),
        ("paragraph_scores", ParagraphScore),
        ("sentence_scores", SentenceScore),
        ("document_overlap_scores", DocumentOverlapScore),
        ("section_overlap_scores", SectionOverlapScore),
        ("paragraph_overlap_scores", ParagraphOverlapScore),
        ("sentence_overlap_scores", SentenceOverlapScore),
        ("document_bm25_scores", DocumentBm25Score),
        ("section_bm25_scores", SectionBm25Score),
        ("paragraph_bm25_scores", ParagraphBm25Score),
        ("sentence_bm25_scores", SentenceBm25Score),
        ("document_similarities", DocumentSimilarity),
        ("section_similarities", SectionSimilarity),
        ("paragraph_similarities", ParagraphSimilarity),
        ("sentence_similarities", SentenceSimilarity),
        ("band_memberships", BandMembership),
        ("user_bands", UserBand),
        ("user_band_memberships", UserBandMembership),
        ("band_fallbacks", BandFallback),
        ("query_document_signals", QueryDocumentSignals),
        ("document_popularity", DocumentPopularity),
    ]
    stages = [step.name.split(".", 1)[0] for step in plan.steps]
    assert stages.index("chunked") < stages.index("indexed")
    assert stages.index("chunked") < stages.index("analyzed") < stages.index("corpus")
    assert stages.index("profiled") < stages.index("analyzed")
    assert stages.index("labeled") < stages.index("scored")
    scoring_steps = [step.name for step in plan.steps if step.name.startswith("scored.")]

    def first(stage: str) -> int:
        return next(index for index, name in enumerate(scoring_steps) if name.startswith(f"scored.{stage}."))

    assert first("popular") < first("recent") < first("offline") < first("scored")
    assert stages.index("indexed") < stages.index("scored")
    assert stages.index("indexed") < stages.index("similarities")
    assert stages.index("cohorts") < stages.index("relevance")


def test_search_all_training_endpoint_builds_features_and_training_data() -> None:
    """The all package exposes a focused offline-training data endpoint."""

    from examples.search.schemas.evaluation.judged_quality import DocumentRelevanceJudgment
    from examples.search.schemas.features import DocumentFeatures, QueryFeatures
    from examples.search.schemas.search import DocumentScore, SearchQuery
    from examples.search.schemas.text import Document
    from examples.search.schemas.training import DocumentTrainingData
    from examples.search.transforms.all.training import Training, TrainingPipeline, training_examples

    plan = Compiler.frontend.compile()(Training, materialize_schemas=False).analysis
    assert isinstance(plan, TransformPlan)
    assert TrainingPipeline.__name__ == "TrainingPipeline"
    assert callable(training_examples)
    assert [(item.name, item.schema) for item in plan.inputs] == [
        ("documents", Document),
        ("queries", SearchQuery),
        ("document_scores", DocumentScore),
        ("judgments", DocumentRelevanceJudgment),
    ]
    assert [(item.name, item.schema) for item in plan.outputs] == [
        ("document_features", DocumentFeatures),
        ("query_features", QueryFeatures),
        ("training_data", DocumentTrainingData),
    ]
    stages = [step.name.split(".", 1)[0] for step in plan.steps]
    assert set(stages) == {"features", "data"}
    assert stages.index("features") < stages.index("data")


def test_search_experiments_replace_production_stages() -> None:
    """Experiment transforms inherit the production graph and replace only their variant stages."""

    from examples.search.transforms.experiments.scoring.Scoring001AdjustBm import Scoring001AdjustBm
    from examples.search.transforms.experiments.searching.search_docs.Searching001AdjustRerankSearchDocuments import (
        Searching001AdjustRerankDocuments,
        Searching001AdjustRerankSearchDocuments,
    )
    from examples.search.transforms.scoring.Scoring import Scoring

    scoring = cast(TransformPlan, Compiler.frontend.compile()(Scoring001AdjustBm, materialize_schemas=False).analysis)
    searching = cast(
        TransformPlan,
        Compiler.frontend.compile()(Searching001AdjustRerankSearchDocuments, materialize_schemas=False).analysis,
    )

    assert {step.name.split(".")[0] for step in scoring.steps} == {"overlap", "bm25", "selected"}
    parameters = getattr(Scoring001AdjustBm.bm25, "_structure_bound_parameters")
    assert parameters["k1"] == 1.35
    assert parameters["b"] == 0.70
    assert Scoring001AdjustBm.experiment_id == "Scoring001AdjustBm"
    assert Scoring001AdjustBm.selected is Scoring.selected
    rerank = next(step for step in searching.steps if step.name == "reranked.score_candidates")
    assert rerank.origin is not None
    assert rerank.origin.class_name == Searching001AdjustRerankDocuments.__name__


def test_similarity_public_module_reexports_search_similarity() -> None:
    """The public module is a thin import surface for the concrete searching transform."""

    from examples.search.transforms.searching.search_similarity import SearchSimilarity
    from examples.search.transforms.similarity import SearchSimilarity as PublicSearchSimilarity

    assert PublicSearchSimilarity is SearchSimilarity


def test_behavior_evaluator_keeps_its_request_to_daily_pipeline_local() -> None:
    """The public evaluator owns all behavior stages for direct IDE navigation."""

    from examples.search.transforms.evaluation.search_docs.behavior.eval_behavior import EvaluateDocSearchBehavior

    assert EvaluateDocSearchBehavior.__bases__ == (Transform,)

    plan = Compiler.frontend.compile()(EvaluateDocSearchBehavior, materialize_schemas=False).analysis
    assert isinstance(plan, TransformPlan)
    assert [output.name for output in plan.outputs] == [
        "request_behaviors",
        "daily_behavior",
    ]
    assert [step.name for step in plan.steps] == [
        "select_requests",
        "select_impressions",
        "count_clicks",
        "measure_impressions",
        "measure_requests",
        "calculate_reciprocal_rank",
        "publish_requests",
        "summarize_exposure",
        "summarize_requests",
        "publish_daily",
        "publish_request_behaviors",
        "publish_daily_behavior",
    ]


def test_experiment_evaluators_schedule_combined_selection_before_their_override() -> None:
    """Experiment evaluators reuse combined selection before adding experiment context."""

    from examples.search.transforms.experiments.evaluation.search_docs.eval_behavior import EvaluateDocSearchBehavior
    from examples.search.transforms.experiments.evaluation.search_docs.eval_ranking import EvaluateDocumentRanking

    for evaluator, parent_step in (
        (EvaluateDocumentRanking, "select_queries"),
        (EvaluateDocSearchBehavior, "select_requests"),
    ):
        plan = cast(TransformPlan, Compiler.frontend.compile()(evaluator, materialize_schemas=False).analysis)
        steps = plan.steps
        parent_origin = steps[0].origin
        child_origin = steps[1].origin

        assert [step.name for step in steps[:2]] == [
            f"Evaluate{'DocumentRanking' if parent_step == 'select_queries' else 'DocSearchBehavior'}.{parent_step}",
            parent_step,
        ]
        assert parent_origin is not None
        assert child_origin is not None
        assert parent_origin.module.startswith("examples.search.transforms.evaluation.search_docs")
        assert child_origin.module.startswith("examples.search.transforms.experiments")


def _paths_diff(actual: dict[str, str], expected: dict[str, str]) -> str:
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    return f"missing generated files: {missing}\nextra generated files: {extra}"


def _text_diff(path: str, expected: str, actual: str) -> str:
    diff = difflib.unified_diff(
        expected.splitlines(),
        actual.splitlines(),
        fromfile=f"examples/{path}",
        tofile=f"actual/{path}",
        lineterm="",
    )
    return "\n".join(diff)
