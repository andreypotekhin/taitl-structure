import csv
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence, cast

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows, single

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
from examples.search.schemas.chunking.intermediate import (
    ExpandedDocumentLine,
    ExpandedSentenceText,
    MarkedDocumentLine,
    MaterializedParagraph,
    MaterializedSection,
    MaterializedSentence,
    ParagraphContent,
    ParagraphDraft,
    ParagraphLine,
    ParagraphLineGroup,
    SectionHeading,
    SectionKey,
    SentenceText,
)
from examples.search.schemas.clicks import Click, DailyClicks, DailyImpressions, Impression, SearchRequest
from examples.search.schemas.cohorts.resolve import BandAncestor, BandMatch, SingletonUserBand, UserBandPath
from examples.search.schemas.evaluation import (
    BehaviorDailyCounts,
    BehaviorExposure,
    BehaviorImpression,
    BehaviorRequest,
    BehaviorRequestMetrics,
    BehaviorRequestTotals,
    DailyDocumentSearchBehavior,
    DocumentEvaluationSummary,
    DocumentQueryEvaluation,
    DocumentRelevanceJudgment,
    DocumentSearchRequestBehavior,
    EvaluationBatch,
    EvaluationIdealDcg,
    EvaluationJudgment,
    EvaluationJudgmentTotals,
    EvaluationParams,
    EvaluationQuery,
    EvaluationResult,
    EvaluationResultTotals,
)
from examples.search.schemas.experiment import Experiment
from examples.search.schemas.features import DocumentFeatures, QueryFeatures
from examples.search.schemas.features.intermediate import (
    ExpandedQueryFeatureToken,
    QueryFeatureToken,
    QueryTokenSummary,
)
from examples.search.schemas.fields import (
    AnalyzerPolicy,
    DocumentField,
    FieldProfile,
    FieldSearchClauseMatch,
    FieldSearchDelegation,
    FieldSearchDocumentMatch,
    FieldSearchQuery,
    FieldSearchResult,
    FieldSearchTerm,
    FieldSearchTermMatch,
    FieldTerm,
)
from examples.search.schemas.fields.intermediate import (
    DocumentFieldEntry,
    ExpandedDocumentField,
    ExpandedFieldText,
    FieldText,
)
from examples.search.schemas.filtering import DocumentFilterMatch, DocumentFilterScore, FilterQueryAvailability
from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexSummary,
    DocumentIndexTarget,
    DocumentTerm,
    ParagraphIndexSummary,
    ParagraphIndexTarget,
    ParagraphTerm,
    SectionIndexSummary,
    SectionIndexTarget,
    SectionTerm,
    SentenceIndexSummary,
    SentenceIndexTarget,
    SentenceTerm,
)
from examples.search.schemas.indexing.lexical.intermediate import (
    DocumentHierarchyCounts,
    DocumentIndexTargetStats,
    DocumentTermCount,
    ExpandedTermText,
    IndexTargetFrequency,
    LexicalOccurrence,
    ParagraphIndexTargetStats,
    ParagraphTermCount,
    SectionIndexTargetStats,
    SectionTermCount,
    SentenceIndexTargetStats,
    SentenceTermCount,
    TermText,
)
from examples.search.schemas.indexing.vector import (
    DocumentVectorCandidate,
    DocumentVectorEmbedding,
    DocumentVectorIndex,
    DocumentVectorIndexSummary,
    DocumentVectorQuery,
    DocumentVectorScore,
    ParagraphVectorEmbedding,
    ParagraphVectorIndex,
    ParagraphVectorIndexSummary,
    ParagraphVectorQuery,
    ParagraphVectorScore,
    SearchQueryVectorEmbedding,
    SimilarityQueryEmbedding,
    VectorIndexPolicy,
)
from examples.search.schemas.label import (
    Intent,
    IntentPattern,
    Label,
    LabelMapEntry,
    QueryIntentLabel,
    QueryLabel,
    QueryLabelAssignmentEntries,
    QueryLabelAssignments,
)
from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from examples.search.schemas.relevance_signals.build import (
    ContextDailyClicks,
    ContextDailyImpressions,
    DocumentPopularityTotals,
    QueryDocumentSignalTotals,
)
from examples.search.schemas.scoring.bm25 import (
    DocumentBm25Score,
    ParagraphBm25Score,
    SectionBm25Score,
    SentenceBm25Score,
)
from examples.search.schemas.scoring.intermediate import (
    DocumentOverlapMatch,
    ExpandedQueryToken,
    ParagraphOverlapMatch,
    PopularQueryCandidate,
    QueryIdfTotal,
    QueryTerm,
    QueryTermCount,
    QueryTermIdf,
    QueryToken,
    ScoreQueryAvailability,
    SectionOverlapMatch,
    SentenceOverlapMatch,
)
from examples.search.schemas.scoring.overlap import (
    DocumentOverlapScore,
    ParagraphOverlapScore,
    SectionOverlapScore,
    SentenceOverlapScore,
)
from examples.search.schemas.search import (
    DocumentFeedbackOption,
    DocumentScore,
    DocumentSearchCandidate,
    DocumentSearchResult,
    DocumentSearchTarget,
    GapPolicy,
    ParagraphContext,
    ParagraphScore,
    ParagraphSearchTarget,
    PassageSearchResult,
    PopularityFeedback,
    QueryDocumentFeedback,
    QueryPopularity,
    ScorePolicy,
    SearchQuery,
    SectionScore,
    SectionSearchTarget,
    SentenceScore,
    SentenceSearchResult,
    SentenceSearchTarget,
)
from examples.search.schemas.similarities.intermediate import (
    DocumentSimilarityCandidate,
    DocumentSimilarityPair,
    DocumentSimilarityQueryText,
    ParagraphSimilarityCandidate,
    ParagraphSimilarityPair,
    ParagraphSimilarityQueryText,
    SectionSimilarityCandidate,
    SectionSimilarityPair,
    SectionSimilarityQueryText,
    SentenceSimilarityCandidate,
    SentenceSimilarityPair,
    SentenceSimilarityQueryText,
)
from examples.search.schemas.similarities.vector import DocumentFusedSimilarityCandidate
from examples.search.schemas.similarity import (
    DocumentSimilarity,
    DocumentSimilarityQuery,
    IndexedSimilarDocument,
    IndexedSimilarParagraph,
    ParagraphSimilarity,
    ParagraphSimilarityQuery,
    SectionSimilarity,
    SectionSimilarityQuery,
    SentenceSimilarity,
    SentenceSimilarityQuery,
    SimilarityFusionPolicy,
    SimilarityParagraphQuery,
    SimilarityPolicy,
    SimilaritySearchQuery,
    SimilaritySectionQuery,
    SimilaritySentenceQuery,
)
from examples.search.schemas.text import Document, Paragraph, Section, Sentence
from examples.search.schemas.training import DocumentTrainingData, RankingArtifact
from examples.search.schemas.user import Band, BandFallback, BandMembership, User, UserBand, UserBandMembership
from examples.search.transforms.all import All, Training
from examples.search.transforms.chunking import Chunking, DocumentChunking, SentenceChunking
from examples.search.transforms.clicks.Clicks import Clicks
from examples.search.transforms.clicks.Impressions import Impressions
from examples.search.transforms.cohorts import ResolveCohortBands
from examples.search.transforms.evaluate import (
    EvaluateAllDocSearchBehavior,
    EvaluateAllDocumentRanking,
    EvaluateDocSearchBehavior,
    EvaluateDocumentRanking,
    EvaluateLabeledDocSearchBehavior,
    EvaluateLabeledDocumentRanking,
    EvaluateUserDocSearchBehavior,
    EvaluateUserDocumentRanking,
)
from examples.search.transforms.experiment import EvaluateDocSearchBehavior as EvaluateExperimentDocSearchBehavior
from examples.search.transforms.experiment import EvaluateDocumentRanking as EvaluateExperimentDocumentRanking
from examples.search.transforms.experiment import (
    Scoring001AdjustBm,
    Searching001AdjustRerankSearchDocuments,
    SelectExperimentScores,
)
from examples.search.transforms.features import BuildDocumentFeatures, BuildQueryFeatures, Features
from examples.search.transforms.fields import ExtractDocumentFields
from examples.search.transforms.indexing import FieldIndex, Indexing
from examples.search.transforms.labeling import CreateQueryLabels, Labeling, MergeQueryLabels
from examples.search.transforms.offline.scoring.lexical.MergeOfflineQueries import MergeOfflineQueries
from examples.search.transforms.offline.scoring.lexical.OfflineScoring import OfflineScoring
from examples.search.transforms.online.scoring.lexical import OnlineScoring
from examples.search.transforms.relevance.BuildRelevanceSignals import BuildRelevanceSignals
from examples.search.transforms.score import Scoring
from examples.search.transforms.scoring.lexical.AllScoringTargets import AllScoringTargets
from examples.search.transforms.scoring.lexical.ScoreBm25 import ScoreBm25
from examples.search.transforms.scoring.lexical.ScoreOverlap import ScoreOverlap
from examples.search.transforms.scoring.lexical.SelectPopularQueries import SelectPopularQueries
from examples.search.transforms.scoring.lexical.SelectRecentQueries import SelectRecentQueries
from examples.search.transforms.search import SearchDocuments, SearchPassages, SearchSentences
from examples.search.transforms.searching.search_fields import SearchFields
from examples.search.transforms.searching.search_similarity import SearchSimilarity
from examples.search.transforms.similarity.lexical.queries import CreateSimilarityQueries
from examples.search.transforms.similarity.lexical.reduce import ReduceSimilarityScores
from examples.search.transforms.similarity.lexical.Similarities import Similarities
from examples.search.transforms.stats.AnalyzeText import AnalyzeText
from examples.search.transforms.stats.CorpusText import CorpusText
from examples.search.transforms.stats.ProfileDocuments import ProfileDocuments
from examples.search.transforms.training import BuildTrainingData, RankDocumentCandidates
from structure import Schema
from structure.plugin.pyspark import TimeWindow

pytestmark = pytest.mark.integration

PACKAGE = "integration_search_generated"
FIXTURES = Path(__file__).resolve().parents[4] / "examples" / "fixtures" / "search"
SCHEMA_MODULES: Mapping[str, Sequence[type[Schema]]] = {
    "examples.search.schemas.filtering": [DocumentFilterMatch, DocumentFilterScore, FilterQueryAvailability],
    "examples.search.schemas.analytics": [
        DocumentProfile,
        SentenceStatistics,
        ParagraphStatistics,
        SectionStatistics,
        DocumentStatistics,
        CorpusStatistics,
        CorpusVocabulary,
        SimilarDocument,
    ],
    "examples.search.schemas.search": [
        SearchQuery,
        DocumentSearchTarget,
        SectionSearchTarget,
        ParagraphSearchTarget,
        SentenceSearchTarget,
        ScorePolicy,
        GapPolicy,
        QueryPopularity,
        SentenceSearchResult,
        PassageSearchResult,
        ParagraphContext,
        DocumentScore,
        SectionScore,
        ParagraphScore,
        SentenceScore,
        DocumentSearchCandidate,
        DocumentFeedbackOption,
        QueryDocumentFeedback,
        PopularityFeedback,
        DocumentSearchResult,
    ],
    "examples.search.schemas.scoring.overlap": [
        DocumentOverlapScore,
        SectionOverlapScore,
        ParagraphOverlapScore,
        SentenceOverlapScore,
    ],
    "examples.search.schemas.scoring.intermediate": [
        QueryToken,
        ExpandedQueryToken,
        QueryIdfTotal,
        QueryTerm,
        QueryTermCount,
        QueryTermIdf,
        ScoreQueryAvailability,
        PopularQueryCandidate,
        DocumentOverlapMatch,
        SectionOverlapMatch,
        ParagraphOverlapMatch,
        SentenceOverlapMatch,
    ],
    "examples.search.schemas.indexing.lexical.index": [
        DocumentIndexTarget,
        SectionIndexTarget,
        ParagraphIndexTarget,
        SentenceIndexTarget,
        DocumentTerm,
        DocumentIndexSummary,
        SectionTerm,
        SectionIndexSummary,
        ParagraphTerm,
        ParagraphIndexSummary,
        SentenceTerm,
        SentenceIndexSummary,
    ],
    "examples.search.schemas.indexing.vector": [
        DocumentVectorEmbedding,
        ParagraphVectorEmbedding,
        DocumentVectorQuery,
        SearchQueryVectorEmbedding,
        SimilarityQueryEmbedding,
        DocumentVectorIndex,
        ParagraphVectorIndex,
        ParagraphVectorQuery,
        DocumentVectorIndexSummary,
        ParagraphVectorIndexSummary,
        DocumentVectorScore,
        DocumentVectorCandidate,
        ParagraphVectorScore,
        VectorIndexPolicy,
    ],
    "examples.search.schemas.fields": [
        AnalyzerPolicy,
        DocumentField,
        FieldProfile,
        FieldSearchDelegation,
        FieldTerm,
        FieldSearchQuery,
        FieldSearchTerm,
        FieldSearchTermMatch,
        FieldSearchClauseMatch,
        FieldSearchDocumentMatch,
        FieldSearchResult,
    ],
    "examples.search.schemas.fields.intermediate": [
        DocumentFieldEntry,
        ExpandedDocumentField,
        FieldText,
        ExpandedFieldText,
    ],
    "examples.search.schemas.indexing.lexical.intermediate": [
        IndexTargetFrequency,
        DocumentTermCount,
        DocumentIndexTargetStats,
        DocumentHierarchyCounts,
        TermText,
        ExpandedTermText,
        LexicalOccurrence,
        SectionTermCount,
        SectionIndexTargetStats,
        ParagraphTermCount,
        ParagraphIndexTargetStats,
        SentenceTermCount,
        SentenceIndexTargetStats,
    ],
    "examples.search.schemas.scoring.bm25": [
        DocumentBm25Score,
        SectionBm25Score,
        ParagraphBm25Score,
        SentenceBm25Score,
    ],
    "examples.search.schemas.clicks": [
        SearchRequest,
        Impression,
        Click,
        DailyImpressions,
        DailyClicks,
    ],
    "examples.search.schemas.experiment": [Experiment],
    "structure.plugin.pyspark.dsl.TimeWindow": [TimeWindow],
    "examples.search.schemas.evaluation.batch": [EvaluationBatch],
    "examples.search.schemas.evaluation.params": [EvaluationParams],
    "examples.search.schemas.label": [
        Intent,
        IntentPattern,
        Label,
        QueryLabel,
        LabelMapEntry,
        QueryIntentLabel,
        QueryLabelAssignmentEntries,
        QueryLabelAssignments,
    ],
    "examples.search.schemas.evaluation.judged_quality": [
        DocumentRelevanceJudgment,
        DocumentQueryEvaluation,
        DocumentEvaluationSummary,
        EvaluationQuery,
        EvaluationResult,
        EvaluationJudgment,
        EvaluationJudgmentTotals,
        EvaluationIdealDcg,
        EvaluationResultTotals,
    ],
    "examples.search.schemas.evaluation.behavior": [
        DocumentSearchRequestBehavior,
        DailyDocumentSearchBehavior,
        BehaviorRequest,
        BehaviorImpression,
        BehaviorExposure,
        BehaviorRequestMetrics,
        BehaviorRequestTotals,
        BehaviorDailyCounts,
    ],
    "examples.search.schemas.training.data": [DocumentTrainingData],
    "examples.search.schemas.training.artifact": [RankingArtifact],
    "examples.search.schemas.features": [
        DocumentFeatures,
        QueryFeatures,
    ],
    "examples.search.schemas.features.intermediate": [
        QueryFeatureToken,
        ExpandedQueryFeatureToken,
        QueryTokenSummary,
    ],
    "examples.search.schemas.relevance": [
        RelevancePolicy,
        QueryDocumentSignals,
        DocumentPopularity,
    ],
    "examples.search.schemas.relevance_signals.build": [
        ContextDailyImpressions,
        ContextDailyClicks,
        QueryDocumentSignalTotals,
        DocumentPopularityTotals,
    ],
    "examples.search.schemas.user": [
        User,
        Band,
        BandMembership,
        BandFallback,
        UserBand,
        UserBandMembership,
    ],
    "examples.search.schemas.cohorts.resolve": [
        BandMatch,
        BandAncestor,
        UserBandPath,
        SingletonUserBand,
    ],
    "examples.search.schemas.similarity": [
        SimilarityPolicy,
        SimilarityFusionPolicy,
        SimilaritySearchQuery,
        SimilaritySectionQuery,
        SimilarityParagraphQuery,
        SimilaritySentenceQuery,
        DocumentSimilarityQuery,
        SectionSimilarityQuery,
        ParagraphSimilarityQuery,
        SentenceSimilarityQuery,
        DocumentSimilarity,
        IndexedSimilarDocument,
        IndexedSimilarParagraph,
        SectionSimilarity,
        ParagraphSimilarity,
        SentenceSimilarity,
    ],
    "examples.search.schemas.similarities.intermediate": [
        DocumentSimilarityCandidate,
        DocumentSimilarityPair,
        SectionSimilarityCandidate,
        SectionSimilarityPair,
        ParagraphSimilarityCandidate,
        ParagraphSimilarityPair,
        SentenceSimilarityCandidate,
        SentenceSimilarityPair,
        DocumentSimilarityQueryText,
        SectionSimilarityQueryText,
        ParagraphSimilarityQueryText,
        SentenceSimilarityQueryText,
    ],
    "examples.search.schemas.similarities.vector": [DocumentFusedSimilarityCandidate],
    "examples.search.schemas.text": [
        Document,
        Section,
        Paragraph,
        Sentence,
    ],
    "examples.search.schemas.chunking.intermediate": [
        ExpandedDocumentLine,
        MarkedDocumentLine,
        ParagraphLine,
        SectionHeading,
        ParagraphLineGroup,
        ParagraphContent,
        ParagraphDraft,
        SectionKey,
        SentenceText,
        MaterializedParagraph,
        MaterializedSection,
        MaterializedSentence,
        ExpandedSentenceText,
    ],
}
TRANSFORMS = (
    (All, "examples.search.transforms.all.all.All"),
    (Chunking, "examples.search.transforms.chunking.Chunking.Chunking"),
    (DocumentChunking, "examples.search.transforms.chunking.DocumentChunking.DocumentChunking"),
    (SentenceChunking, "examples.search.transforms.chunking.SentenceChunking.SentenceChunking"),
    (ProfileDocuments, "examples.search.transforms.stats.ProfileDocuments.ProfileDocuments"),
    (BuildDocumentFeatures, "examples.search.transforms.features.BuildDocumentFeatures.BuildDocumentFeatures"),
    (BuildQueryFeatures, "examples.search.transforms.features.BuildQueryFeatures.BuildQueryFeatures"),
    (Features, "examples.search.transforms.features.Features.Features"),
    (BuildTrainingData, "examples.search.transforms.training.BuildTrainingData.BuildTrainingData"),
    (RankDocumentCandidates, "examples.search.transforms.training.RankDocumentCandidates.RankDocumentCandidates"),
    (Training, "examples.search.transforms.training.Training.Training"),
    (AnalyzeText, "examples.search.transforms.stats.AnalyzeText.AnalyzeText"),
    (CorpusText, "examples.search.transforms.stats.CorpusText.CorpusText"),
    (Indexing, "examples.search.transforms.indexing.Indexing.Indexing"),
    (ExtractDocumentFields, "examples.search.transforms.fields.ExtractDocumentFields.ExtractDocumentFields"),
    (FieldIndex, "examples.search.transforms.indexing.fields.FieldIndex.FieldIndex"),
    (SearchFields, "examples.search.transforms.searching.search_fields.SearchFields.SearchFields"),
    (SearchSentences, "examples.search.transforms.searching.search_sentences.SearchSentences.SearchSentences"),
    (SearchPassages, "examples.search.transforms.searching.search_passages.SearchPassages.SearchPassages"),
    (Impressions, "examples.search.transforms.clicks.Impressions.Impressions"),
    (Clicks, "examples.search.transforms.clicks.Clicks.Clicks"),
    (
        BuildRelevanceSignals,
        "examples.search.transforms.relevance.BuildRelevanceSignals.BuildRelevanceSignals",
    ),
    (SearchDocuments, "examples.search.transforms.searching.search_docs.SearchDocuments.SearchDocuments"),
    (
        Searching001AdjustRerankSearchDocuments,
        "examples.search.transforms.experiments.searching.search_docs.Searching001AdjustRerankSearchDocuments.Searching001AdjustRerankSearchDocuments",
    ),
    (MergeQueryLabels, "examples.search.transforms.labeling.MergeQueryLabels.MergeQueryLabels"),
    (CreateQueryLabels, "examples.search.transforms.labeling.CreateQueryLabels.CreateQueryLabels"),
    (Labeling, "examples.search.transforms.labeling.Labeling.Labeling"),
    (
        SelectExperimentScores,
        "examples.search.transforms.experiments.SelectExperimentScores.SelectExperimentScores",
    ),
    (
        EvaluateExperimentDocumentRanking,
        "examples.search.transforms.experiments.evaluation.search_docs.eval_ranking.EvaluateDocumentRanking",
    ),
    (
        EvaluateExperimentDocSearchBehavior,
        "examples.search.transforms.experiments.evaluation.search_docs.eval_behavior.EvaluateDocSearchBehavior",
    ),
    (
        EvaluateDocumentRanking,
        "examples.search.transforms.evaluation.search_docs.ranking.eval_ranking.EvaluateDocumentRanking",
    ),
    (
        EvaluateDocSearchBehavior,
        "examples.search.transforms.evaluation.search_docs.behavior.eval_behavior.EvaluateDocSearchBehavior",
    ),
    (
        EvaluateLabeledDocumentRanking,
        "examples.search.transforms.evaluation.search_docs.ranking.with_labels.EvaluateDocumentRanking",
    ),
    (
        EvaluateLabeledDocSearchBehavior,
        "examples.search.transforms.evaluation.search_docs.behavior.with_labels.EvaluateDocSearchBehavior",
    ),
    (
        EvaluateUserDocumentRanking,
        "examples.search.transforms.evaluation.search_docs.ranking.with_users.EvaluateDocumentRanking",
    ),
    (
        EvaluateUserDocSearchBehavior,
        "examples.search.transforms.evaluation.search_docs.behavior.with_users.EvaluateDocSearchBehavior",
    ),
    (
        EvaluateAllDocumentRanking,
        "examples.search.transforms.evaluation.search_docs.ranking.with_all.EvaluateDocumentRanking",
    ),
    (
        EvaluateAllDocSearchBehavior,
        "examples.search.transforms.evaluation.search_docs.behavior.with_all.EvaluateDocSearchBehavior",
    ),
    (
        CreateSimilarityQueries,
        "examples.search.transforms.similarity.lexical.queries.CreateSimilarityQueries",
    ),
    (ScoreOverlap, "examples.search.transforms.scoring.lexical.ScoreOverlap.ScoreOverlap"),
    (ScoreBm25, "examples.search.transforms.scoring.lexical.ScoreBm25.ScoreBm25"),
    (Scoring, "examples.search.transforms.scoring.Scoring.Scoring"),
    (OfflineScoring, "examples.search.transforms.offline.scoring.lexical.OfflineScoring.OfflineScoring"),
    (MergeOfflineQueries, "examples.search.transforms.offline.scoring.lexical.MergeOfflineQueries.MergeOfflineQueries"),
    (SelectPopularQueries, "examples.search.transforms.scoring.lexical.SelectPopularQueries.SelectPopularQueries"),
    (SelectRecentQueries, "examples.search.transforms.scoring.lexical.SelectRecentQueries.SelectRecentQueries"),
    (OnlineScoring, "examples.search.transforms.online.scoring.lexical.OnlineScoring.OnlineScoring"),
    (
        Scoring001AdjustBm,
        "examples.search.transforms.experiments.scoring.Scoring001AdjustBm.Scoring001AdjustBm",
    ),
    (
        ReduceSimilarityScores,
        "examples.search.transforms.similarity.lexical.reduce.ReduceSimilarityScores",
    ),
    (Similarities, "examples.search.transforms.similarity.lexical.Similarities"),
    (
        SearchSimilarity,
        "examples.search.transforms.searching.search_similarity.SearchSimilarity.SearchSimilarity",
    ),
    (ResolveCohortBands, "examples.search.transforms.cohorts.ResolveCohortBands.ResolveCohortBands"),
)


def test_query_labeling_pipeline_renders_with_stage_owned_raw_hook() -> None:
    files = render_generated_project(
        Labeling,
        source_transform="examples.search.transforms.labeling.Labeling.Labeling",
        generated_package=PACKAGE,
        source_schema_modules=SCHEMA_MODULES,
    )

    text = files[f"{PACKAGE}/pyspark/transforms/examples/search/transforms/labeling/Labeling.py"]
    assert "from examples.search.transforms.labeling.CreateQueryLabels import CreateQueryLabels" in text
    assert "match_patterns(" in text
    assert "merge_created_labels" in text


def test_query_intents_create_multilingual_english_labels_online_and_generated(spark, tmp_path) -> None:
    files = render_generated_project(
        CreateQueryLabels,
        source_transform="examples.search.transforms.labeling.CreateQueryLabels.CreateQueryLabels",
        generated_package=PACKAGE,
        source_schema_modules=SCHEMA_MODULES,
    )
    files.update(
        render_generated_project(
            MergeQueryLabels,
            source_transform="examples.search.transforms.labeling.MergeQueryLabels.MergeQueryLabels",
            generated_package=PACKAGE,
            source_schema_modules=SCHEMA_MODULES,
        )
    )
    with generated_project(tmp_path, PACKAGE, files):
        labels = __import__(f"{PACKAGE}.pyspark.schemas.label", fromlist=["INTENT_SCHEMA"])
        search = __import__(f"{PACKAGE}.pyspark.schemas.search", fromlist=["SEARCH_QUERY_SCHEMA"])
        queries = spark.createDataFrame(
            [
                (
                    "q-en",
                    "natural",
                    "What is the current status?",
                    datetime(2026, 7, 21, 9),
                    {"is_question": 0, "is_time_sensitive": 0, "tier": 2},
                    False,
                    False,
                    None,
                ),
                (
                    "q-uk",
                    "natural",
                    "What is the status?",
                    datetime(2026, 7, 21, 9),
                    {"tier": 1},
                    False,
                    False,
                    "en_UK",
                ),
                ("q-es", "natural", "¿Qué es Structure?", datetime(2026, 7, 21, 9), {"tier": 3}, False, True, "es_ES"),
            ],
            search.SEARCH_QUERY_SCHEMA,
        )
        intents = spark.createDataFrame(
            [("question", "is_question"), ("time_sensitive", "is_time_sensitive")], labels.INTENT_SCHEMA
        )
        patterns = spark.createDataFrame(
            [
                ("question", "en_US", r"^(what|how|why|when|where|who|is|are|can|do|does)\b|.*\?$"),
                ("question", "en_UK", r"^(what|how|why|when|where|who|is|are|can|do|does)\b|.*\?$"),
                ("question", "es_ES", r"^(qué|como|cómo|cuándo|dónde|quién|es|son)\b|.*\?$"),
                ("time_sensitive", "en_US", r"\b(current|latest|today)\b"),
                ("time_sensitive", "es_ES", r"\b(actual|hoy)\b"),
            ],
            labels.INTENT_PATTERN_SCHEMA,
        )
        empty_labels = spark.createDataFrame([], labels.QUERY_LABEL_SCHEMA)
        inputs = dict(queries=queries, intents=intents, patterns=patterns)
        online_created = CreateQueryLabels(**inputs).run(session(spark, execution_mode="online"))
        generated_created = CreateQueryLabels(**inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        online = MergeQueryLabels(queries=queries, query_labels=empty_labels, created_labels=online_created.labels).run(
            session(spark, execution_mode="online")
        )
        generated = MergeQueryLabels(
            queries=queries, query_labels=empty_labels, created_labels=generated_created.labels
        ).run(session(spark, execution_mode="generated", generated_package=PACKAGE))

        expected = [
            {
                "id": "q-en",
                "queryset": "natural",
                "content": "What is the current status?",
                "requested_at": datetime(2026, 7, 21, 9),
                "labels": {"is_question": 1, "is_time_sensitive": 1, "tier": 2},
                "is_question": True,
                "is_time_sensitive": True,
                "language": None,
            },
            {
                "id": "q-es",
                "queryset": "natural",
                "content": "¿Qué es Structure?",
                "requested_at": datetime(2026, 7, 21, 9),
                "labels": {"is_question": 1, "is_time_sensitive": 0, "tier": 3},
                "is_question": True,
                "is_time_sensitive": False,
                "language": "es_ES",
            },
            {
                "id": "q-uk",
                "queryset": "natural",
                "content": "What is the status?",
                "requested_at": datetime(2026, 7, 21, 9),
                "labels": {"is_question": 1, "is_time_sensitive": 0, "tier": 1},
                "is_question": True,
                "is_time_sensitive": False,
                "language": "en_UK",
            },
        ]
        assert rows(online_created.labels, "query_id") == rows(generated_created.labels, "query_id")
        assert rows(online.labeled_queries, "id") == expected
        assert rows(generated.labeled_queries, "id") == expected


def test_text_fixture_runs_online_and_generated(spark, tmp_path, cache_frames) -> None:
    files = {}
    for transform, source in TRANSFORMS:
        files.update(
            render_generated_project(
                transform,
                source_transform=source,
                generated_package=PACKAGE,
                source_schema_modules=SCHEMA_MODULES,
            )
        )

    with generated_project(tmp_path, PACKAGE, files):
        schemas = __import__(f"{PACKAGE}.pyspark.schemas.text", fromlist=["DOCUMENT_SCHEMA"])
        documents = spark.createDataFrame(
            [
                (
                    "d-1",
                    "docs",
                    "structure",
                    "Structure Guide",
                    "https://example.test/guide",
                    "# Introduction\nStructure makes typed Spark transforms readable.\n\n# Analysis\nThis guide measures text clearly.",
                    "text/plain",
                    "utf-8",
                    "en",
                    datetime(2026, 7, 1, 8),
                    datetime(2026, 7, 2, 8),
                    datetime(2026, 7, 10, 9),
                    None,
                    None,
                    None,
                    "guide",
                    "docs",
                    ".md",
                    {
                        "source": "structure",
                        "title": "Structure Guide",
                        "url": "https://example.test/guide",
                        "content_type": "text/plain",
                        "encoding": "utf-8",
                        "language": "en",
                        "document_type": "guide",
                        "category_id": "docs",
                        "file_type": ".md",
                    },
                ),
                (
                    "d-2",
                    "docs",
                    "structure",
                    "Structure Guide Revised",
                    "https://example.test/guide-revised",
                    "# Introduction\nStructure makes typed Spark transformations readable.\n\n# Analysis\nThis guide measures similarity clearly.",
                    "text/plain",
                    "utf-8",
                    "en",
                    datetime(2026, 7, 3, 8),
                    datetime(2026, 7, 4, 8),
                    datetime(2026, 7, 10, 10),
                    None,
                    None,
                    None,
                    "guide",
                    "docs",
                    ".md",
                    {
                        "source": "structure",
                        "title": "Structure Guide Revised",
                        "url": "https://example.test/guide-revised",
                        "content_type": "text/plain",
                        "encoding": "utf-8",
                        "language": "en",
                        "document_type": "guide",
                        "category_id": "docs",
                        "file_type": ".md",
                    },
                ),
                (
                    "d-3",
                    "docs",
                    "reference",
                    "Reference Notes",
                    None,
                    "Plain text has one paragraph. It still becomes a document section.",
                    "text/plain",
                    "utf-8",
                    "en",
                    None,
                    None,
                    datetime(2026, 7, 11, 11),
                    None,
                    None,
                    None,
                    "reference",
                    "docs",
                    ".txt",
                    {
                        "source": "reference",
                        "title": "Reference Notes",
                        "content_type": "text/plain",
                        "encoding": "utf-8",
                        "language": "en",
                        "document_type": "reference",
                        "category_id": "docs",
                        "file_type": ".txt",
                    },
                ),
            ],
            schemas.DOCUMENT_SCHEMA,
        )
        generated_segments = Chunking(documents=documents).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert generated_segments.sections.columns == [
            "id",
            "document_id",
            "ordinal",
            "span_start",
            "span_end",
            "heading_span_start",
            "heading_span_end",
        ]
        assert generated_segments.paragraphs.columns == [
            "id",
            "document_id",
            "section_id",
            "ordinal",
            "span_start",
            "span_end",
        ]
        assert generated_segments.sentences.columns == [
            "id",
            "document_id",
            "section_id",
            "paragraph_id",
            "paragraph_ordinal",
            "ordinal",
            "span_start",
            "span_end",
        ]
        assert len(rows(generated_segments.sections, "id")) == 5
        document_section = single(generated_segments.sections, lambda row: row["document_id"] == "d-3")
        assert document_section["heading_span_start"] is None
        assert document_section["heading_span_end"] is None
        assert document_section["span_start"] == 0
        assert document_section["span_end"] == len("Plain text has one paragraph. It still becomes a document section.")

        online_features = ProfileDocuments(documents=documents).run(session(spark, execution_mode="online")).features
        generated_features = (
            ProfileDocuments(documents=documents)
            .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
            .features
        )
        cache_frames(
            generated_segments.sentences,
            generated_segments.paragraphs,
            generated_segments.sections,
            generated_features,
        )
        assert rows(online_features, "document_id") == rows(generated_features, "document_id")
        guide = single(generated_features, lambda row: row["document_id"] == "d-1")
        assert guide["url_is_https"] is True
        assert guide["content_contains_structure"] is True
        assert guide["content_sha2"]

        online_index = _run_indexing(spark, documents, generated_segments.sentences, execution_mode="online")
        generated_index = _run_indexing(
            spark, documents, generated_segments.sentences, execution_mode="generated", generated_package=PACKAGE
        )
        inputs = dict(
            documents=documents,
            sentences=generated_segments.sentences,
            paragraphs=generated_segments.paragraphs,
            sections=generated_segments.sections,
            document_terms=generated_index.document_terms,
            section_terms=generated_index.section_terms,
            paragraph_terms=generated_index.paragraph_terms,
            sentence_terms=generated_index.sentence_terms,
            comparison_left=generated_features,
            comparison_right=generated_features,
        )
        online_analytics = AnalyzeText(**inputs).run(session(spark, execution_mode="online"))
        generated_analytics = AnalyzeText(**inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_analytics.document_statistics, "document_id") == rows(
            generated_analytics.document_statistics, "document_id"
        )
        assert rows(online_analytics.sentence_statistics, "sentence_id") == rows(
            generated_analytics.sentence_statistics, "sentence_id"
        )
        assert {row["sentence_id"] for row in rows(generated_analytics.sentence_statistics)} == {
            row["id"] for row in rows(generated_segments.sentences)
        }
        first_sentence = single(
            generated_segments.sentences,
            lambda row: row["id"] == "d-1#p0#s0",
        )
        first_statistics = single(
            generated_analytics.sentence_statistics,
            lambda row: row["sentence_id"] == first_sentence["id"],
        )
        assert first_statistics["ordinal"] == first_sentence["ordinal"] == 1
        assert first_statistics["word_count"] == 6
        assert len(rows(generated_analytics.similar_documents, "left_document_id", "right_document_id")) == 1

        corpus_inputs = dict(
            document_statistics=generated_analytics.document_statistics,
            document_terms=generated_index.document_terms,
        )
        online_corpus = CorpusText(**corpus_inputs).run(session(spark, execution_mode="online"))
        generated_corpus = CorpusText(**corpus_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_corpus.corpus_statistics, "corpus") == rows(generated_corpus.corpus_statistics, "corpus")
        assert rows(online_corpus.corpus_vocabulary, "corpus") == rows(generated_corpus.corpus_vocabulary, "corpus")

        queries = spark.createDataFrame(
            [
                (
                    "q-structure",
                    "natural",
                    "Structure transformations",
                    datetime(2026, 7, 21, 9),
                    {"is_question": 0, "is_time_sensitive": 0},
                    False,
                    False,
                    None,
                ),
                (
                    "q-reference",
                    "natural",
                    "reference text",
                    datetime(2026, 7, 21, 9),
                    {"is_question": 0, "is_time_sensitive": 0},
                    False,
                    False,
                    None,
                ),
            ],
            __import__(f"{PACKAGE}.pyspark.schemas.search", fromlist=["SEARCH_QUERY_SCHEMA"]).SEARCH_QUERY_SCHEMA,
        )
        cache_frames(
            generated_index.document_terms,
            generated_index.document_summary,
            generated_index.section_terms,
            generated_index.section_summary,
            generated_index.paragraph_terms,
            generated_index.paragraph_summary,
            generated_index.sentence_terms,
            generated_index.sentence_summary,
        )
        assert rows(online_index.document_terms, "document_id", "term") == rows(
            generated_index.document_terms, "document_id", "term"
        )
        document_structure = single(
            generated_index.document_terms,
            lambda row: row["document_id"] == "d-1" and row["term"] == "structure",
        )
        assert document_structure["term_frequency"] == 1
        assert document_structure["target_frequency"] == 2
        assert cast(int, document_structure["target_term_count"]) >= cast(
            int, document_structure["target_distinct_term_count"]
        )
        assert single(generated_index.document_summary, lambda row: True)["target_count"] == 3
        assert single(generated_index.sentence_summary, lambda row: True)["target_count"] == len(
            rows(generated_segments.sentences, "id")
        )

        similarity_policy = spark.createDataFrame(
            [(None,)],
            __import__(
                f"{PACKAGE}.pyspark.schemas.similarity", fromlist=["SIMILARITY_POLICY_SCHEMA"]
            ).SIMILARITY_POLICY_SCHEMA,
        )
        similarity_index_inputs = dict(
            policy=similarity_policy,
            document_terms=generated_index.document_terms,
            document_summary=generated_index.document_summary,
            section_terms=generated_index.section_terms,
            section_summary=generated_index.section_summary,
            paragraph_terms=generated_index.paragraph_terms,
            paragraph_summary=generated_index.paragraph_summary,
            sentence_terms=generated_index.sentence_terms,
            sentence_summary=generated_index.sentence_summary,
        )
        online_similarity_queries = CreateSimilarityQueries(**similarity_index_inputs).run(
            session(spark, execution_mode="online")
        )
        generated_similarity_queries = CreateSimilarityQueries(**similarity_index_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_similarity_queries.queries, "id") == rows(generated_similarity_queries.queries, "id")
        assert len(rows(generated_similarity_queries.document_queries, "query_id")) == 3
        assert (
            single(generated_similarity_queries.document_queries, lambda row: row["document_id"] == "d-1")["query_id"]
            == "document:d-1"
        )
        pruned_policy = spark.createDataFrame(
            [(0.5,)],
            __import__(
                f"{PACKAGE}.pyspark.schemas.similarity", fromlist=["SIMILARITY_POLICY_SCHEMA"]
            ).SIMILARITY_POLICY_SCHEMA,
        )
        pruned_queries = CreateSimilarityQueries(**{**similarity_index_inputs, "policy": pruned_policy}).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert (
            "structure"
            not in cast(str, single(pruned_queries.queries, lambda row: row["id"] == "document:d-1")["content"]).split()
        )

        similarity_score_inputs = dict(
            queries=generated_similarity_queries.queries,
            targets=AllScoringTargets(
                queries=generated_similarity_queries.queries,
                document_terms=generated_index.document_terms,
            ).run(session(spark, execution_mode="generated", generated_package=PACKAGE)).targets,
            **{name: value for name, value in similarity_index_inputs.items() if name != "policy"},
        )
        score_policy = spark.createDataFrame(
            [(30, datetime(2026, 7, 21), datetime(2026, 7, 21), 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)],
            __import__(f"{PACKAGE}.pyspark.schemas.search", fromlist=["SCORE_POLICY_SCHEMA"]).SCORE_POLICY_SCHEMA,
        )
        similarity_overlap_inputs = dict(similarity_score_inputs)
        similarity_overlap_inputs["score_policy"] = score_policy
        online_similarity_overlap_scores = ScoreOverlap(**similarity_overlap_inputs).run(
            session(spark, execution_mode="online")
        )
        online_similarity_bm25_scores = ScoreBm25(**similarity_score_inputs).run(
            session(spark, execution_mode="online")
        )
        generated_similarity_overlap_scores = ScoreOverlap(**similarity_overlap_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        generated_similarity_bm25_scores = ScoreBm25(**similarity_score_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        similarity_reducer_inputs = dict(
            document_queries=generated_similarity_queries.document_queries,
            section_queries=generated_similarity_queries.section_queries,
            paragraph_queries=generated_similarity_queries.paragraph_queries,
            sentence_queries=generated_similarity_queries.sentence_queries,
            **{
                name: getattr(generated_similarity_overlap_scores, name)
                for name in (
                    "document_overlap_scores",
                    "section_overlap_scores",
                    "paragraph_overlap_scores",
                    "sentence_overlap_scores",
                )
            },
            **{
                name: getattr(generated_similarity_bm25_scores, name)
                for name in (
                    "document_bm25_scores",
                    "section_bm25_scores",
                    "paragraph_bm25_scores",
                    "sentence_bm25_scores",
                )
            },
        )
        online_similarities = ReduceSimilarityScores(
            document_queries=online_similarity_queries.document_queries,
            section_queries=online_similarity_queries.section_queries,
            paragraph_queries=online_similarity_queries.paragraph_queries,
            sentence_queries=online_similarity_queries.sentence_queries,
            **{
                name: getattr(online_similarity_overlap_scores, name)
                for name in (
                    "document_overlap_scores",
                    "section_overlap_scores",
                    "paragraph_overlap_scores",
                    "sentence_overlap_scores",
                )
            },
            **{
                name: getattr(online_similarity_bm25_scores, name)
                for name in (
                    "document_bm25_scores",
                    "section_bm25_scores",
                    "paragraph_bm25_scores",
                    "sentence_bm25_scores",
                )
            },
        ).run(session(spark, execution_mode="online"))
        generated_similarities = ReduceSimilarityScores(**similarity_reducer_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_similarities.document_similarities, "left_document_id", "right_document_id") == rows(
            generated_similarities.document_similarities, "left_document_id", "right_document_id"
        )
        similar_guides = single(
            generated_similarities.document_similarities,
            lambda row: row["left_document_id"] == "d-1" and row["right_document_id"] == "d-2",
        )
        reverse_similar_guides = single(
            generated_similarities.document_similarities,
            lambda row: row["left_document_id"] == "d-2" and row["right_document_id"] == "d-1",
        )
        assert cast(float, similar_guides["score_overlap"]) > 0
        assert cast(float, similar_guides["bm25_left_to_right"]) > 0
        assert cast(float, similar_guides["bm25_right_to_left"]) > 0
        assert similar_guides["rank"] == 1
        assert reverse_similar_guides["rank"] == 1
        document_neighbor_rows = rows(generated_similarities.document_similarities, "left_document_id", "rank")
        for source_id in {row["left_document_id"] for row in document_neighbor_rows}:
            ranks = [row["rank"] for row in document_neighbor_rows if row["left_document_id"] == source_id]
            assert len(ranks) <= ReduceSimilarityScores.maximum_results
            assert ranks == list(range(1, len(ranks) + 1))

        similar_document_inputs = dict(
            query=documents.where("id = 'd-1'"),
            documents=documents,
            document_similarities=generated_similarities.document_similarities,
        )
        vector_schemas = __import__(
            f"{PACKAGE}.pyspark.schemas.indexing_vector",
            fromlist=[
                "SIMILARITY_QUERY_EMBEDDING_SCHEMA",
                "DOCUMENT_VECTOR_INDEX_SCHEMA",
                "VECTOR_INDEX_POLICY_SCHEMA",
            ],
        )
        similarity_schemas = __import__(
            f"{PACKAGE}.pyspark.schemas.similarity", fromlist=["SIMILARITY_FUSION_POLICY_SCHEMA"]
        )
        fusion_policy = spark.createDataFrame(
            [(60, 10, 10, 10, "lexical-regression")],
            similarity_schemas.SIMILARITY_FUSION_POLICY_SCHEMA,
        )
        empty_query_embeddings = spark.createDataFrame(
            [], vector_schemas.SIMILARITY_QUERY_EMBEDDING_SCHEMA
        )
        empty_document_index = spark.createDataFrame([], vector_schemas.DOCUMENT_VECTOR_INDEX_SCHEMA)
        vector_policy = spark.createDataFrame(
            [("fixture-embed", 3, "rev-1", "search-v1", 1000, 60)],
            vector_schemas.VECTOR_INDEX_POLICY_SCHEMA,
        )
        similar_document_inputs.update(
            query_vector_embeddings=empty_query_embeddings,
            document_vector_index=empty_document_index,
            score_policy=score_policy,
            vector_policy=vector_policy,
            fusion_policy=fusion_policy,
        )
        online_similar_documents = (
            SearchSimilarity(
                **{**similar_document_inputs, "document_similarities": online_similarities.document_similarities}
            )
            .run(session(spark, execution_mode="online"))
            .similar_documents
        )
        generated_similar_documents = (
            SearchSimilarity(**similar_document_inputs)
            .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
            .similar_documents
        )
        assert rows(online_similar_documents, "rank", "id") == rows(generated_similar_documents, "rank", "id")
        best_match = single(generated_similar_documents, lambda row: row["rank"] == 1)
        assert best_match["id"] == "d-2"
        assert best_match["search_query_id"] == "d-1"

        ann_query_embeddings = spark.createDataFrame(
            [([1.0, 0.0, 0.0], "fixture-embed", 3, "rev-1", "search-v1", "d-1")],
            vector_schemas.SIMILARITY_QUERY_EMBEDDING_SCHEMA,
        )
        ann_document_index = spark.createDataFrame(
            [
                ([1.0, 0.0, 0.0], "fixture-embed", 3, "rev-1", "search-v1", "d-1"),
                ([0.8, 0.6, 0.0], "fixture-embed", 3, "rev-1", "search-v1", "d-2"),
                ([0.99, 0.1, 0.0], "fixture-embed", 3, "rev-1", "search-v1", "d-3"),
            ],
            vector_schemas.DOCUMENT_VECTOR_INDEX_SCHEMA,
        )
        hybrid_inputs = {
            **similar_document_inputs,
            "query_vector_embeddings": ann_query_embeddings,
            "document_vector_index": ann_document_index,
        }
        online_hybrid_documents = (
            SearchSimilarity(
                **{**hybrid_inputs, "document_similarities": online_similarities.document_similarities}
            )
            .run(session(spark, execution_mode="online"))
            .similar_documents
        )
        generated_hybrid_documents = (
            SearchSimilarity(**hybrid_inputs)
            .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
            .similar_documents
        )
        assert rows(online_hybrid_documents, "rank", "id") == rows(generated_hybrid_documents, "rank", "id")
        ann_match = single(generated_hybrid_documents, lambda row: row["id"] == "d-3")
        assert ann_match["vector_backend"] == "exact_reference"
        assert ann_match["vector_rank"] == 1
        assert ann_match["vector_similarity"] == pytest.approx(0.99 / (0.99**2 + 0.1**2) ** 0.5)
        assert ann_match["rrf_k"] == 60

        search_inputs = dict(
            queries=queries,
            targets=AllScoringTargets(
                queries=queries,
                document_terms=generated_index.document_terms,
            ).run(session(spark, execution_mode="generated", generated_package=PACKAGE)).targets,
            document_terms=generated_index.document_terms,
            document_summary=generated_index.document_summary,
            section_terms=generated_index.section_terms,
            section_summary=generated_index.section_summary,
            paragraph_terms=generated_index.paragraph_terms,
            paragraph_summary=generated_index.paragraph_summary,
            sentence_terms=generated_index.sentence_terms,
            sentence_summary=generated_index.sentence_summary,
            score_policy=score_policy,
        )
        online_scores = Scoring(**search_inputs).run(session(spark, execution_mode="online"))
        generated_scores = Scoring(**search_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_scores.document_scores, "query_id", "document_id") == rows(
            generated_scores.document_scores, "query_id", "document_id"
        )
        assert rows(online_scores.section_scores, "query_id", "section_id") == rows(
            generated_scores.section_scores, "query_id", "section_id"
        )
        structure_document = single(
            generated_scores.document_scores,
            lambda row: row["query_id"] == "q-structure" and row["document_id"] == "d-1",
        )
        assert structure_document["experiment_id"] is None
        assert cast(float, structure_document["score"]) > 0


def test_search_ranks_fixture_sentences_online_and_generated(spark, tmp_path) -> None:
    files = {}
    for transform, source in TRANSFORMS:
        files.update(
            render_generated_project(
                transform,
                source_transform=source,
                generated_package=PACKAGE,
                source_schema_modules=SCHEMA_MODULES,
            )
        )

    with generated_project(tmp_path, PACKAGE, files):
        text_schemas = __import__(f"{PACKAGE}.pyspark.schemas.text", fromlist=["DOCUMENT_SCHEMA"])
        search_schemas = __import__(
            f"{PACKAGE}.pyspark.schemas.search",
            fromlist=["SEARCH_QUERY_SCHEMA", "DOCUMENT_SCORE_SCHEMA"],
        )
        documents = spark.createDataFrame(_search_documents(), text_schemas.DOCUMENT_SCHEMA)
        queries = spark.createDataFrame(
            [
                (
                    "q-aurora",
                    "natural",
                    "aurora beacon navigation",
                    datetime(2026, 7, 21, 9),
                    {"is_question": 0, "is_time_sensitive": 0},
                    False,
                    False,
                    None,
                ),
                (
                    "q-free-form",
                    "natural",
                    "  AURORA,   beacon! navigation?  ",
                    datetime(2026, 7, 21, 9),
                    {"is_question": 0, "is_time_sensitive": 0},
                    False,
                    False,
                    None,
                ),
            ],
            search_schemas.SEARCH_QUERY_SCHEMA,
        )
        segments = Chunking(documents=documents).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        index = _run_indexing(spark, documents, segments.sentences, execution_mode="generated", generated_package=PACKAGE)
        scores = Scoring(
            queries=queries,
            targets=AllScoringTargets(queries=queries, document_terms=index.document_terms).run(
                session(spark, execution_mode="generated", generated_package=PACKAGE)
            ).targets,
            document_terms=index.document_terms,
            document_summary=index.document_summary,
            section_terms=index.section_terms,
            section_summary=index.section_summary,
            paragraph_terms=index.paragraph_terms,
            paragraph_summary=index.paragraph_summary,
            sentence_terms=index.sentence_terms,
            sentence_summary=index.sentence_summary,
            score_policy=spark.createDataFrame(
                [(30, datetime(2026, 7, 21), datetime(2026, 7, 21), 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)],
                search_schemas.SCORE_POLICY_SCHEMA,
            ),
        ).run(session(spark, execution_mode="generated", generated_package=PACKAGE))

        inputs = dict(
            queries=queries,
            documents=documents,
            sentences=segments.sentences,
            sentence_scores=scores.sentence_scores,
        )
        online = SearchSentences(**inputs).run(session(spark, execution_mode="online")).results
        generated = (
            SearchSentences(**inputs).run(session(spark, execution_mode="generated", generated_package=PACKAGE)).results
        )

        assert generated.columns == [
            "search_query_id",
            "experiment_id",
            "rank",
            "document_id",
            "section_id",
            "paragraph_id",
            "sentence_id",
            "content",
            "score",
        ]
        assert rows(online, "search_query_id", "rank") == rows(generated, "search_query_id", "rank")
        results = rows(generated, "rank")
        for query_id in ("q-aurora", "q-free-form"):
            matches = [row for row in results if row["search_query_id"] == query_id]
            assert [row["rank"] for row in matches] == [1, 2, 3]
            assert [row["sentence_id"] for row in matches] == ["d-11#p0#s0", "d-12#p0#s0", "d-13#p0#s0"]
            assert all(row["score"] is not None for row in matches)

        exact = [row for row in results if row["search_query_id"] == "q-aurora"]
        free_form = [row for row in results if row["search_query_id"] == "q-free-form"]
        assert [row["score"] for row in free_form] == [row["score"] for row in exact]
        assert cast(float, exact[0]["score"]) > cast(float, exact[1]["score"]) > cast(float, exact[2]["score"])


def test_passage_search_ranks_paragraphs_with_same_section_context(spark, tmp_path) -> None:
    files = {}
    for transform, source in TRANSFORMS:
        files.update(
            render_generated_project(
                transform,
                source_transform=source,
                generated_package=PACKAGE,
                source_schema_modules=SCHEMA_MODULES,
            )
        )

    with generated_project(tmp_path, PACKAGE, files):
        text_schemas = __import__(f"{PACKAGE}.pyspark.schemas.text", fromlist=["DOCUMENT_SCHEMA"])
        search_schemas = __import__(
            f"{PACKAGE}.pyspark.schemas.search", fromlist=["SEARCH_QUERY_SCHEMA", "DOCUMENT_SCORE_SCHEMA"]
        )
        documents = spark.createDataFrame(
            [
                (
                    "d-passage",
                    "docs",
                    "structure",
                    "Passage Guide",
                    "https://example.test/passage-guide",
                    "# Guide\nSignal starts the guide.\n\nSignal continues the guide.\n\n"
                    "Signal closes the guide.\n\n# Boundary\nSignal starts the next section.",
                    "text/plain",
                    "utf-8",
                    "en",
                    None,
                    None,
                    datetime(2026, 7, 22, 9),
                    None,
                    None,
                    None,
                    "guide",
                    "docs",
                    ".md",
                    {
                        "source": "structure",
                        "title": "Passage Guide",
                        "url": "https://example.test/passage-guide",
                        "content_type": "text/plain",
                        "encoding": "utf-8",
                        "language": "en",
                        "document_type": "guide",
                        "category_id": "docs",
                        "file_type": ".md",
                    },
                )
            ],
            text_schemas.DOCUMENT_SCHEMA,
        )
        queries = spark.createDataFrame(
            [
                (
                    "q-free-form",
                    "natural",
                    "  SIGNAL!  ",
                    datetime(2026, 7, 21, 9),
                    {"is_question": 0, "is_time_sensitive": 0},
                    False,
                    False,
                    None,
                ),
                (
                    "q-boundary",
                    "natural",
                    "next section",
                    datetime(2026, 7, 21, 9),
                    {"is_question": 0, "is_time_sensitive": 0},
                    False,
                    False,
                    None,
                ),
            ],
            search_schemas.SEARCH_QUERY_SCHEMA,
        )
        segments = Chunking(documents=documents).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        index = _run_indexing(spark, documents, segments.sentences, execution_mode="generated", generated_package=PACKAGE)
        scores = Scoring(
            queries=queries,
            targets=AllScoringTargets(queries=queries, document_terms=index.document_terms).run(
                session(spark, execution_mode="generated", generated_package=PACKAGE)
            ).targets,
            document_terms=index.document_terms,
            document_summary=index.document_summary,
            section_terms=index.section_terms,
            section_summary=index.section_summary,
            paragraph_terms=index.paragraph_terms,
            paragraph_summary=index.paragraph_summary,
            sentence_terms=index.sentence_terms,
            sentence_summary=index.sentence_summary,
            score_policy=spark.createDataFrame(
                [(30, datetime(2026, 7, 22), datetime(2026, 7, 22), 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)],
                search_schemas.SCORE_POLICY_SCHEMA,
            ),
        ).run(session(spark, execution_mode="generated", generated_package=PACKAGE))

        inputs = dict(
            queries=queries,
            paragraph_scores=scores.paragraph_scores,
            paragraphs=segments.paragraphs,
            sections=segments.sections,
            documents=documents,
        )
        online = SearchPassages(**inputs).run(session(spark, execution_mode="online")).results
        generated = (
            SearchPassages(**inputs).run(session(spark, execution_mode="generated", generated_package=PACKAGE)).results
        )

        assert generated.columns == [
            "search_query_id",
            "experiment_id",
            "rank",
            "document_id",
            "title",
            "url",
            "section_id",
            "section_heading",
            "paragraph_id",
            "preceding_content",
            "content",
            "following_content",
            "score",
        ]
        assert rows(online, "search_query_id", "rank") == rows(generated, "search_query_id", "rank")
        signal_results = [
            row for row in rows(generated, "search_query_id", "rank") if row["search_query_id"] == "q-free-form"
        ]
        assert [row["rank"] for row in signal_results] == [1, 2, 3, 4]
        guide = {row["content"]: row for row in signal_results if row["section_heading"] == "Guide"}
        assert len(guide) == 3
        assert guide["Signal starts the guide."]["preceding_content"] is None
        assert guide["Signal starts the guide."]["following_content"] == "Signal continues the guide."
        assert guide["Signal continues the guide."]["preceding_content"] == "Signal starts the guide."
        assert guide["Signal continues the guide."]["following_content"] == "Signal closes the guide."
        assert guide["Signal closes the guide."]["preceding_content"] == "Signal continues the guide."
        assert guide["Signal closes the guide."]["following_content"] is None
        boundary = single(generated, lambda row: row["search_query_id"] == "q-boundary")
        assert boundary["preceding_content"] is None
        assert boundary["following_content"] is None
        assert boundary["title"] == "Passage Guide"
        assert boundary["url"] == "https://example.test/passage-guide"
        assert boundary["section_heading"] == "Boundary"


def test_relevance_signals_keep_binary_ctr_separate_from_engagement(spark, tmp_path) -> None:
    files = {}
    for transform, source in TRANSFORMS:
        files.update(
            render_generated_project(
                transform,
                source_transform=source,
                generated_package=PACKAGE,
                source_schema_modules=SCHEMA_MODULES,
            )
        )

    with generated_project(tmp_path, PACKAGE, files):
        click_schemas = __import__(f"{PACKAGE}.pyspark.schemas.clicks", fromlist=["DAILY_IMPRESSIONS_SCHEMA"])
        relevance_schemas = __import__(f"{PACKAGE}.pyspark.schemas.relevance", fromlist=["RELEVANCE_POLICY_SCHEMA"])
        user_schemas = __import__(
            f"{PACKAGE}.pyspark.schemas.user",
            fromlist=["BAND_FALLBACK_SCHEMA", "BAND_MEMBERSHIP_SCHEMA", "USER_BAND_MEMBERSHIP_SCHEMA"],
        )
        start = datetime(2026, 7, 20)
        end = datetime(2026, 7, 21)
        daily_impressions = spark.createDataFrame(
            [
                ((start, end), "aurora", "d-active", 1, 0.25, None, None, 10),
                ((start, end), "aurora", "d-active", 2, 0.5, None, None, 10),
                ((start, end), "aurora", "d-low", 1, 0.5, None, None, 19),
            ],
            click_schemas.DAILY_IMPRESSIONS_SCHEMA,
        )
        daily_clicks = spark.createDataFrame(
            [
                ((start, end), "aurora", "d-active", 1, 0.25, None, None, 6, 5, 120.0, 2.0, 2),
                ((start, end), "aurora", "d-active", 2, 0.5, None, None, 0, 0, 0.0, 0.0, 0),
                ((start, end), "aurora", "d-low", 1, 0.5, None, None, 2, 1, 60.0, 1.0, 1),
            ],
            click_schemas.DAILY_CLICKS_SCHEMA,
        )
        policy = spark.createDataFrame(
            [(30.0, 0.7, 0.3, 0.7, 0.3, 20, 20, datetime(2026, 7, 21))],
            relevance_schemas.RELEVANCE_POLICY_SCHEMA,
        )
        inputs = dict(
            daily_impressions=daily_impressions,
            daily_clicks=daily_clicks,
            band_memberships=spark.createDataFrame([], user_schemas.BAND_MEMBERSHIP_SCHEMA),
            user_band_memberships=spark.createDataFrame([], user_schemas.USER_BAND_MEMBERSHIP_SCHEMA),
            band_fallbacks=spark.createDataFrame([], user_schemas.BAND_FALLBACK_SCHEMA),
            policy=policy,
        )
        online = BuildRelevanceSignals(**inputs).run(session(spark, execution_mode="online"))
        generated = BuildRelevanceSignals(**inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )

        assert rows(online.query_document_signals, "query", "document_id") == rows(
            generated.query_document_signals, "query", "document_id"
        )
        active = single(generated.query_document_signals, lambda row: row["document_id"] == "d-active")
        low = single(generated.query_document_signals, lambda row: row["document_id"] == "d-low")
        assert active["click_count"] == 6
        assert active["clicked_impression_count"] == 5
        assert active["click_through_rate"] == pytest.approx(0.25)
        assert active["ips_click_through_rate"] == pytest.approx(1.0 / 3.0)
        assert active["normalized_ctr_score"] == pytest.approx(1.0 / 3.0)
        assert low["click_count"] == 2
        assert low["clicked_impression_count"] == 1
        assert low["normalized_ctr_score"] == 0.0


def test_document_search_reranks_bm25_candidates_for_multiple_queries(spark, tmp_path) -> None:
    files = {}
    for transform, source in TRANSFORMS:
        files.update(
            render_generated_project(
                transform,
                source_transform=source,
                generated_package=PACKAGE,
                source_schema_modules=SCHEMA_MODULES,
            )
        )

    with generated_project(tmp_path, PACKAGE, files):
        text_schemas = __import__(f"{PACKAGE}.pyspark.schemas.text", fromlist=["DOCUMENT_SCHEMA"])
        search_schemas = __import__(
            f"{PACKAGE}.pyspark.schemas.search", fromlist=["SEARCH_QUERY_SCHEMA", "DOCUMENT_SCORE_SCHEMA"]
        )
        vector_schemas = __import__(
            f"{PACKAGE}.pyspark.schemas.indexing_vector",
            fromlist=[
                "DOCUMENT_SEARCH_VECTOR_EMBEDDING_SCHEMA",
                "DOCUMENT_VECTOR_INDEX_SCHEMA",
                "PARAGRAPH_VECTOR_INDEX_SCHEMA",
                "PARAGRAPH_VECTOR_QUERY_SCHEMA",
                "VECTOR_INDEX_POLICY_SCHEMA",
            ],
        )
        overlap_schemas = __import__(f"{PACKAGE}.pyspark.schemas.overlap", fromlist=["DOCUMENT_OVERLAP_SCORE_SCHEMA"])
        filtering_schemas = __import__(
            f"{PACKAGE}.pyspark.schemas.filtering", fromlist=["DOCUMENT_FILTER_SCORE_SCHEMA"]
        )
        relevance_schemas = __import__(f"{PACKAGE}.pyspark.schemas.relevance", fromlist=["RELEVANCE_POLICY_SCHEMA"])
        click_schemas = __import__(f"{PACKAGE}.pyspark.schemas.clicks", fromlist=["SEARCH_REQUEST_SCHEMA"])
        user_schemas = __import__(
            f"{PACKAGE}.pyspark.schemas.user",
            fromlist=["BAND_FALLBACK_SCHEMA", "COHORT_MEMBERSHIP_SCHEMA"],
        )
        queries = spark.createDataFrame(
            [
                (
                    "q-free-form",
                    "natural",
                    "  AURORA,   beacon!  ",
                    datetime(2026, 7, 21, 9),
                    {"is_question": 0, "is_time_sensitive": 0},
                    False,
                    False,
                    None,
                ),
                (
                    "q-navigation",
                    "natural",
                    "navigation",
                    datetime(2026, 7, 21, 9),
                    {"is_question": 0, "is_time_sensitive": 0},
                    False,
                    False,
                    None,
                ),
            ],
            search_schemas.SEARCH_QUERY_SCHEMA,
        )
        scores = {"d-11": 10.0, "d-12": 9.0, "d-13": 8.0}
        documents = spark.createDataFrame(_search_documents(), text_schemas.DOCUMENT_SCHEMA)
        segments = Chunking(documents=documents).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        index = _run_indexing(spark, documents, segments.sentences, execution_mode="generated", generated_package=PACKAGE)
        scored_at = datetime(2026, 7, 21)
        document_score_rows = [
            (query_id, cast(str, row[0]), None, scored_at, scores[cast(str, row[0])])
            for query_id in ("q-free-form", "q-navigation")
            for row in _search_documents()
        ]
        document_scores = spark.createDataFrame(document_score_rows, search_schemas.DOCUMENT_SCORE_SCHEMA)
        document_overlap_scores = spark.createDataFrame(
            [
                (query_id, cast(str, row[0]), scored_at, scores[cast(str, row[0])])
                for query_id in ("q-free-form", "q-navigation")
                for row in _search_documents()
            ],
            overlap_schemas.DOCUMENT_OVERLAP_SCORE_SCHEMA,
        )
        document_filter_scores = spark.createDataFrame(
            [
                (query_id, cast(str, row[0]), scored_at, 1, rank)
                for query_id in ("q-free-form", "q-navigation")
                for rank, row in enumerate(_search_documents(), start=1)
            ],
            filtering_schemas.DOCUMENT_FILTER_SCORE_SCHEMA,
        )
        query_signals = spark.createDataFrame(
            [
                ("aurora, beacon!", "d-12", None, 1, 1, 1, 60.0, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
                ("navigation", "d-13", None, 1, 1, 1, 60.0, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            ],
            relevance_schemas.QUERY_DOCUMENT_SIGNALS_SCHEMA,
        )
        popularity = spark.createDataFrame(
            [("d-12", None, 1, 1, 1, 60.0, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)],
            relevance_schemas.DOCUMENT_POPULARITY_SCHEMA,
        )
        policy = spark.createDataFrame(
            [(30.0, 0.7, 0.3, 0.7, 0.3, 20, 20, datetime(2026, 7, 21))],
            relevance_schemas.RELEVANCE_POLICY_SCHEMA,
        )
        inputs = dict(
            queries=queries,
            documents=documents,
            document_scores=document_scores,
            document_vector_scores=spark.createDataFrame([], search_schemas.DOCUMENT_VECTOR_SCORE_SCHEMA),
            streamed_documents=spark.createDataFrame([], text_schemas.DOCUMENT_SCHEMA),
            streamed_document_scores=spark.createDataFrame([], search_schemas.DOCUMENT_SCORE_SCHEMA),
            document_overlap_scores=document_overlap_scores,
            document_filter_scores=document_filter_scores,
            document_terms=index.document_terms,
            document_summary=index.document_summary,
            score_policy=spark.createDataFrame(
                [(30, scored_at, scored_at, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)],
                search_schemas.SCORE_POLICY_SCHEMA,
            ),
            gap_policy=spark.createDataFrame(
                [(True, True, True, False)],
                search_schemas.GAP_POLICY_SCHEMA,
            ),
            document_vector_embeddings=spark.createDataFrame(
                [], vector_schemas.DOCUMENT_SEARCH_VECTOR_EMBEDDING_SCHEMA
            ),
            document_vector_index=spark.createDataFrame([], vector_schemas.DOCUMENT_VECTOR_INDEX_SCHEMA),
            vector_policy=spark.createDataFrame(
                [("fixture-embed", 3, "rev-1", "search-v1", 1000, 60)],
                vector_schemas.VECTOR_INDEX_POLICY_SCHEMA,
            ),
            inference_policy=spark.createDataFrame(
                [("default", "fixture-embed", "default-v1", "rev-1", 3, "search-v1", scored_at)],
                __import__(f"{PACKAGE}.pyspark.schemas.inference", fromlist=["INFERENCE_POLICY_SCHEMA"]).INFERENCE_POLICY_SCHEMA,
            ),
            requests=spark.createDataFrame(
                [
                    ("r-free-form", "q-free-form", "aurora, beacon!", None, None, "", datetime(2026, 7, 21, 9)),
                    ("r-navigation", "q-navigation", "navigation", None, None, "", datetime(2026, 7, 21, 9)),
                ],
                click_schemas.SEARCH_REQUEST_SCHEMA,
            ),
            band_memberships=spark.createDataFrame([], user_schemas.BAND_MEMBERSHIP_SCHEMA),
            band_fallbacks=spark.createDataFrame([], user_schemas.BAND_FALLBACK_SCHEMA),
            query_document_signals=query_signals,
            document_popularity=popularity,
            policy=policy,
        )
        online = SearchDocuments(**inputs).run(session(spark, execution_mode="online")).results
        generated = (
            SearchDocuments(**inputs).run(session(spark, execution_mode="generated", generated_package=PACKAGE)).results
        )

        assert rows(online, "search_query_id", "rank") == rows(generated, "search_query_id", "rank")
        assert [
            row["rank"] for row in rows(generated, "search_query_id", "rank") if row["search_query_id"] == "q-free-form"
        ] == [
            1,
            2,
            3,
        ]
        assert (
            single(generated, lambda row: row["search_query_id"] == "q-free-form" and row["rank"] == 1)["document_id"]
            == "d-12"
        )
        assert (
            single(generated, lambda row: row["search_query_id"] == "q-navigation" and row["rank"] == 1)["document_id"]
            == "d-13"
        )


def _search_documents() -> list[tuple[object, ...]]:
    with (FIXTURES / "similarity.csv").open(newline="", encoding="utf-8") as source:
        return [
            (
                row["id"],
                row["collection_id"],
                row["source"],
                row["title"],
                row["url"] or None,
                row["content"],
                row["content_type"],
                row["encoding"],
                row["language"],
                _timestamp(row["created_at"]),
                _timestamp(row["published_at"]),
                _timestamp(row["harvested_at"]),
                None,
                None,
                None,
                None,
                row["collection_id"],
                ".txt",
                {
                    "source": row["source"],
                    "title": row["title"],
                    **({"url": row["url"]} if row["url"] else {}),
                    "content_type": row["content_type"],
                    "encoding": row["encoding"],
                    "language": row["language"],
                    "category_id": row["collection_id"],
                    "file_type": ".txt",
                },
            )
            for row in csv.DictReader(source)
            if row["id"] in {"d-11", "d-12", "d-13"}
        ]


def _run_indexing(spark, documents, sentences, *, execution_mode: str, generated_package: str | None = None):
    schemas = __import__(
        f"{generated_package or PACKAGE}.pyspark.schemas.fields",
        fromlist=["ANALYZER_POLICY_SCHEMA", "FIELD_PROFILE_SCHEMA"],
    )
    extracted = ExtractDocumentFields(source_documents=documents).run(
        session(spark, execution_mode=execution_mode, generated_package=generated_package)
    )
    field_profiles = spark.createDataFrame(
        [("*", "text", "metadata_text_v1", False, True)],
        schemas.FIELD_PROFILE_SCHEMA,
    )
    analyzer_policies = spark.createDataFrame(
        [("metadata_text_v1", "v1", [])],
        schemas.ANALYZER_POLICY_SCHEMA,
    )
    return Indexing(
        documents=documents,
        sentences=sentences,
        document_fields=extracted.document_fields,
        field_profiles=field_profiles,
        analyzer_policies=analyzer_policies,
    ).run(session(spark, execution_mode=execution_mode, generated_package=generated_package))


def _timestamp(value: str) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
