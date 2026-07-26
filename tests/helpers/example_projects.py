from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence, cast

from structure import *
from structure.core.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.core.compiler.api import Compiler
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.docs.api import Docs
from structure.core.dsl.model.schemas.Schema import Schema
from structure.plugin.pyspark import *
from structure.plugin.pyspark import PySpark
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan

ROOT = Path(".")
EXAMPLES = ROOT / "examples"


def render_store_example() -> dict[str, str]:
    with _example_imports():
        from examples.store.schemas.adv_analytics import (
            OrderCollectionProfile,
            OrderCollectionSource,
            OrderCustomerWindow,
            OrderProductCube,
            OrderRevenueRollup,
        )
        from examples.store.schemas.analytics import CustomerDailyTotal, CustomerEventRank, ProductDailySummary
        from examples.store.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
        from examples.store.schemas.customer import Customer
        from examples.store.schemas.order import (
            CustomerOrderBackfill,
            OrderCustomerReconciliation,
            OrderFulfillment,
            OrderNormalized,
            OrderProductCandidate,
            OrderPublication,
            OrderPublished,
            OrderRaw,
            OrderWithCustomer,
            OrderWithProduct,
            OrderWithPromotion,
            PublicationFlags,
        )
        from examples.store.schemas.product import BlockedProduct, Product, ProductBase
        from examples.store.schemas.promotion import Promotion
        from examples.store.schemas.shipment import Shipment
        from examples.store.transforms.adv_analytics import AdvancedOrderAnalytics
        from examples.store.transforms.analytics import OrderAnalytics
        from examples.store.transforms.order import EnrichOrders
        from examples.store.transforms.rowset_join import RowsetJoinExamples

        schema_modules: dict[str, Sequence[type[Schema]]] = {
            "examples.store.schemas.adv_analytics": [
                OrderRevenueRollup,
                OrderProductCube,
                OrderCustomerWindow,
                OrderCollectionSource,
                OrderCollectionProfile,
            ],
            "examples.store.schemas.analytics": [CustomerDailyTotal, ProductDailySummary, CustomerEventRank],
            "examples.store.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
            "examples.store.schemas.customer": [Customer],
            "examples.store.schemas.order": [
                OrderRaw,
                OrderNormalized,
                OrderWithCustomer,
                OrderWithProduct,
                OrderWithPromotion,
                OrderFulfillment,
                OrderPublication,
                PublicationFlags,
                OrderPublished,
                OrderCustomerReconciliation,
                CustomerOrderBackfill,
                OrderProductCandidate,
            ],
            "examples.store.schemas.product": [ProductBase, Product, BlockedProduct],
            "examples.store.schemas.promotion": [Promotion],
            "examples.store.schemas.shipment": [Shipment],
        }
        files = {}
        transforms = (
            (EnrichOrders, "examples.store.transforms.order.EnrichOrders"),
            (RowsetJoinExamples, "examples.store.transforms.rowset_join.RowsetJoinExamples"),
            (OrderAnalytics, "examples.store.transforms.analytics.OrderAnalytics"),
            (AdvancedOrderAnalytics, "examples.store.transforms.adv_analytics.AdvancedOrderAnalytics"),
        )
        for transform_class, source_transform in transforms:
            files.update(
                PySpark.render.project()(
                    cast(
                        PySparkExecutionPlan,
                        Compiler.frontend.compile()(
                            transform_class,
                            materialize_schemas=False,
                            target_profile=None,
                        ).lowered,
                    ),
                    source_transform=source_transform,
                    generated_package="examples.structure_generated.store",
                    source_schema_modules=schema_modules,
                )
            )
        docs = Docs.render.project()(
            StructureConfig.resolve(
                project_root=ROOT,
                source_roots=["examples"],
                generated_dir="examples/structure_generated/store",
                generated_package="examples.structure_generated.store",
            ),
            DiscoveredStructureProject(
                transforms=tuple(transform for transform, _ in transforms),
                schema_modules={module: tuple(schemas) for module, schemas in schema_modules.items()},
            ),
        )
        files.update({f"examples/structure_generated/store/{path}": text for path, text in docs.items()})
        files["examples/structure_generated/store/traceability/__init__.py"] = (
            "# Generated traceability package marker.\n"
        )
        files["examples/structure_generated/store/traceability/transforms/__init__.py"] = (
            "# Generated transform traceability package marker.\n"
        )
        return files


def expected_store_generated() -> dict[str, str]:
    return _expected_generated("store")


def render_streams_example() -> dict[str, str]:
    with _example_imports():
        from examples.streams.schemas.events import GateProgress, JudgeCall, Passage, Penalty, RawEvent
        from examples.streams.schemas.race import Gate, Paddler, Race, RaceWinner
        from examples.streams.transforms.passages import PreparePassages
        from examples.streams.transforms.penalties import CorrelatePenalties
        from examples.streams.transforms.progress import BuildGateProgress

        schema_modules: dict[str, Sequence[type[Schema]]] = {
            "examples.streams.schemas.events": [RawEvent, Passage, JudgeCall, GateProgress, Penalty],
            "examples.streams.schemas.race": [Race, Gate, Paddler, RaceWinner],
        }
        transforms = (
            (PreparePassages, "examples.streams.transforms.passages.PreparePassages"),
            (BuildGateProgress, "examples.streams.transforms.progress.BuildGateProgress"),
            (CorrelatePenalties, "examples.streams.transforms.penalties.CorrelatePenalties"),
        )
        files = {}
        for transform_class, source_transform in transforms:
            files.update(
                PySpark.render.project()(
                    cast(
                        PySparkExecutionPlan,
                        Compiler.frontend.compile()(transform_class, materialize_schemas=False).lowered,
                    ),
                    source_transform=source_transform,
                    generated_package="examples.structure_generated.streams",
                    source_schema_modules=schema_modules,
                )
            )
        docs = Docs.render.project()(
            StructureConfig.resolve(
                project_root=ROOT,
                source_roots=["examples"],
                generated_dir="examples/structure_generated/streams",
                generated_package="examples.structure_generated.streams",
            ),
            DiscoveredStructureProject(
                transforms=tuple(transform for transform, _ in transforms),
                schema_modules={module: tuple(schemas) for module, schemas in schema_modules.items()},
            ),
        )
        files.update({f"examples/structure_generated/streams/{path}": text for path, text in docs.items()})
        files["examples/structure_generated/streams/traceability/__init__.py"] = (
            "# Generated traceability package marker.\n"
        )
        files["examples/structure_generated/streams/traceability/transforms/__init__.py"] = (
            "# Generated transform traceability package marker.\n"
        )
        return files


def expected_streams_generated() -> dict[str, str]:
    return _expected_generated("streams")


def render_stocks_example() -> dict[str, str]:
    with _example_imports():
        from examples.stocks.schemas.indicators import (
            AdvancedIndicator,
            MomentumIndicator,
            TrendIndicator,
            VolatilityIndicator,
            VolumeIndicator,
        )
        from examples.stocks.schemas.market import BenchmarkReturn, DailyReturn, MarketBar
        from examples.stocks.transforms.advanced import Advanced
        from examples.stocks.transforms.momentum import Momentum
        from examples.stocks.transforms.returns import PrepareReturns
        from examples.stocks.transforms.trend import Trend
        from examples.stocks.transforms.volatility import Volatility
        from examples.stocks.transforms.volume import Volume

        schema_modules: dict[str, Sequence[type[Schema]]] = {
            "examples.stocks.schemas.indicators": [
                TrendIndicator,
                MomentumIndicator,
                VolatilityIndicator,
                VolumeIndicator,
                AdvancedIndicator,
            ],
            "examples.stocks.schemas.market": [MarketBar, DailyReturn, BenchmarkReturn],
        }
        transforms = (
            (PrepareReturns, "examples.stocks.transforms.returns.PrepareReturns"),
            (Trend, "examples.stocks.transforms.trend.Trend"),
            (Momentum, "examples.stocks.transforms.momentum.Momentum"),
            (Volatility, "examples.stocks.transforms.volatility.Volatility"),
            (Volume, "examples.stocks.transforms.volume.Volume"),
            (Advanced, "examples.stocks.transforms.advanced.Advanced"),
        )
        files = {}
        for transform_class, source_transform in transforms:
            files.update(
                PySpark.render.project()(
                    cast(
                        PySparkExecutionPlan,
                        Compiler.frontend.compile()(transform_class, materialize_schemas=False).lowered,
                    ),
                    source_transform=source_transform,
                    generated_package="examples.structure_generated.stocks",
                    source_schema_modules=schema_modules,
                )
            )
        docs = Docs.render.project()(
            StructureConfig.resolve(
                project_root=ROOT,
                source_roots=["examples"],
                generated_dir="examples/structure_generated/stocks",
                generated_package="examples.structure_generated.stocks",
            ),
            DiscoveredStructureProject(
                transforms=tuple(transform for transform, _ in transforms),
                schema_modules={module: tuple(schemas) for module, schemas in schema_modules.items()},
            ),
        )
        files.update({f"examples/structure_generated/stocks/{path}": text for path, text in docs.items()})
        files["examples/structure_generated/stocks/traceability/__init__.py"] = (
            "# Generated traceability package marker.\n"
        )
        files["examples/structure_generated/stocks/traceability/transforms/__init__.py"] = (
            "# Generated transform traceability package marker.\n"
        )
        return {path: text.rstrip() + "\n" for path, text in files.items()}


def expected_stocks_generated() -> dict[str, str]:
    return _expected_generated("stocks")


def render_security_example() -> dict[str, str]:
    with _example_imports():
        from examples.security.schemas.assets import OS, App, Device, DeviceType, Scanner, Software
        from examples.security.schemas.events import AppEvent, RawEvent, VulnEvent
        from examples.security.schemas.organization import Department, Org, Person, Team
        from examples.security.schemas.reporting import (
            AppAuditEvent,
            DepartmentActiveVulnerability,
            DepartmentVulnerabilityStatistic,
            DeviceActiveVulnerability,
            OrgActiveVulnerability,
            OrgVulnerabilityStatistic,
            PersonActiveVulnerability,
            PersonVulnerabilityStatistic,
            ReportingPeriod,
            TeamActiveVulnerability,
            TeamVulnerabilityStatistic,
            VulnerabilityAuditEvent,
            VulnerabilityExposure,
            VulnerabilityInventoryCandidate,
            VulnerabilityInventoryCheck,
            VulnerabilityInventoryIssue,
            VulnerabilityLifecycle,
            VulnerabilityPeriodActivity,
            VulnerabilityPostureCandidate,
            VulnerabilityQualityCheck,
            VulnerabilityQualityIssue,
            VulnerabilityStatistic,
        )
        from examples.security.schemas.risk import Vuln, VulnType
        from examples.security.transforms.events import EnrichAppEvents, EnrichVulnerabilityEvents
        from examples.security.transforms.posture import SecurityPosture
        from examples.security.transforms.quality import SecurityInventoryQuality
        from examples.security.transforms.reports import ActiveVulnerabilityReports, VulnerabilityStatistics

        schema_modules: dict[str, Sequence[type[Schema]]] = {
            "examples.security.schemas.assets": [DeviceType, Software, App, OS, Scanner, Device],
            "examples.security.schemas.events": [RawEvent, AppEvent, VulnEvent],
            "examples.security.schemas.organization": [Org, Department, Team, Person],
            "examples.security.schemas.reporting": [
                ReportingPeriod,
                VulnerabilityExposure,
                DeviceActiveVulnerability,
                PersonActiveVulnerability,
                TeamActiveVulnerability,
                DepartmentActiveVulnerability,
                OrgActiveVulnerability,
                VulnerabilityStatistic,
                PersonVulnerabilityStatistic,
                TeamVulnerabilityStatistic,
                DepartmentVulnerabilityStatistic,
                OrgVulnerabilityStatistic,
                VulnerabilityLifecycle,
                VulnerabilityPeriodActivity,
                VulnerabilityPostureCandidate,
                VulnerabilityQualityCheck,
                VulnerabilityQualityIssue,
                VulnerabilityInventoryCandidate,
                VulnerabilityInventoryCheck,
                VulnerabilityInventoryIssue,
                AppAuditEvent,
                VulnerabilityAuditEvent,
            ],
            "examples.security.schemas.risk": [VulnType, Vuln],
        }
        transforms = (
            (EnrichAppEvents, "examples.security.transforms.events.EnrichAppEvents"),
            (EnrichVulnerabilityEvents, "examples.security.transforms.events.EnrichVulnerabilityEvents"),
            (SecurityPosture, "examples.security.transforms.posture.SecurityPosture"),
            (ActiveVulnerabilityReports, "examples.security.transforms.reports.ActiveVulnerabilityReports"),
            (VulnerabilityStatistics, "examples.security.transforms.reports.VulnerabilityStatistics"),
            (SecurityInventoryQuality, "examples.security.transforms.quality.SecurityInventoryQuality"),
        )
        files = {}
        for transform_class, source_transform in transforms:
            files.update(
                PySpark.render.project()(
                    cast(
                        PySparkExecutionPlan,
                        Compiler.frontend.compile()(transform_class, materialize_schemas=False).lowered,
                    ),
                    source_transform=source_transform,
                    generated_package="examples.structure_generated.security",
                    source_schema_modules=schema_modules,
                )
            )
        docs = Docs.render.project()(
            StructureConfig.resolve(
                project_root=ROOT,
                source_roots=["examples"],
                generated_dir="examples/structure_generated/security",
                generated_package="examples.structure_generated.security",
            ),
            DiscoveredStructureProject(
                transforms=tuple(transform for transform, _ in transforms),
                schema_modules={module: tuple(schemas) for module, schemas in schema_modules.items()},
            ),
        )
        files.update({f"examples/structure_generated/security/{path}": text for path, text in docs.items()})
        files["examples/structure_generated/security/traceability/__init__.py"] = (
            "# Generated traceability package marker.\n"
        )
        files["examples/structure_generated/security/traceability/transforms/__init__.py"] = (
            "# Generated transform traceability package marker.\n"
        )
        return {path: text.rstrip() + "\n" for path, text in files.items()}


def expected_security_generated() -> dict[str, str]:
    return _expected_generated("security")


def render_search_example() -> dict[str, str]:
    with _example_imports():
        from examples.search.schemas.analytics import (
            CorpusStatistics,
            CorpusVocabulary,
            DocumentFeatures,
            DocumentStatistics,
            ParagraphStatistics,
            SectionStatistics,
            SentenceStatistics,
            SimilarDocument,
        )
        from examples.search.schemas.clicks import Click, DailyClicks, DailyImpressions, Impression, SearchRequest
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
        from examples.search.schemas.label import (
            Label,
            LabelMapEntry,
            QueryLabel,
            QueryLabelAssignmentEntries,
            QueryLabelAssignments,
        )
        from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
        from examples.search.schemas.search import (
            DocumentBm25Score,
            DocumentIndexSummary,
            DocumentIndexTarget,
            DocumentIndexTerm,
            DocumentOverlapScore,
            DocumentScore,
            DocumentSearchCandidate,
            DocumentSearchResult,
            DocumentSearchTarget,
            ParagraphBm25Score,
            ParagraphIndexSummary,
            ParagraphIndexTarget,
            ParagraphIndexTerm,
            ParagraphOverlapScore,
            ParagraphScore,
            ParagraphSearchTarget,
            SearchQuery,
            SectionBm25Score,
            SectionIndexSummary,
            SectionIndexTarget,
            SectionIndexTerm,
            SectionOverlapScore,
            SectionScore,
            SectionSearchTarget,
            SentenceBm25Score,
            SentenceIndexSummary,
            SentenceIndexTarget,
            SentenceIndexTerm,
            SentenceOverlapScore,
            SentenceScore,
            SentenceSearchResult,
            SentenceSearchTarget,
        )
        from examples.search.schemas.similarity import (
            DocumentSimilarity,
            DocumentSimilarityQuery,
            IndexedSimilarDocument,
            IndexedSimilarParagraph,
            IndexedSimilarSection,
            IndexedSimilarSentence,
            ParagraphSimilarity,
            ParagraphSimilarityQuery,
            SectionSimilarity,
            SectionSimilarityQuery,
            SentenceSimilarity,
            SentenceSimilarityQuery,
            SimilarityDocumentQuery,
            SimilarityParagraphQuery,
            SimilarityPolicy,
            SimilaritySectionQuery,
            SimilaritySentenceQuery,
        )
        from examples.search.schemas.text import Document, Paragraph, Section, Sentence, Word
        from examples.search.transforms.analyze import AnalyzeText
        from examples.search.transforms.clicks.Clicks import Clicks
        from examples.search.transforms.clicks.Impressions import Impressions
        from examples.search.transforms.corpus import CorpusText
        from examples.search.transforms.evaluate import (
            EvaluateDocumentRankingQuality,
            EvaluateDocumentSearchBehavior,
            EvaluateLabeledDocumentRankingQuality,
            EvaluateLabeledDocumentSearchBehavior,
        )
        from examples.search.transforms.experiment import (
            EvaluateDocumentRankingQuality as EvaluateExperimentDocumentRankingQuality,
        )
        from examples.search.transforms.experiment import (
            EvaluateDocumentSearchBehavior as EvaluateExperimentDocumentSearchBehavior,
        )
        from examples.search.transforms.experiment import SelectExperimentScores
        from examples.search.transforms.extract import ExtractText
        from examples.search.transforms.index import CreateIndex
        from examples.search.transforms.labeling import MergeQueryLabels
        from examples.search.transforms.profile import ProfileDocuments
        from examples.search.transforms.relevance.BuildRelevanceSignals import BuildRelevanceSignals
        from examples.search.transforms.score import AddScores
        from examples.search.transforms.scoring.ScoreAll import ScoreAll
        from examples.search.transforms.search import SearchDocuments, SearchSentences
        from examples.search.transforms.similarities.CreateSimilarityQueries import CreateSimilarityQueries
        from examples.search.transforms.similarities.ReduceSimilarityScores import ReduceSimilarityScores
        from examples.search.transforms.similarities.SimilarParagraphs import SimilarParagraphs
        from examples.search.transforms.similarities.SimilarSections import SimilarSections
        from examples.search.transforms.similarities.SimilarSentences import SimilarSentences
        from examples.search.transforms.similarity import Similarity
        from structure.plugin.pyspark import TimeWindow

        schema_modules: dict[str, Sequence[type[Schema]]] = {
            "examples.search.schemas.analytics": [
                DocumentFeatures,
                SentenceStatistics,
                ParagraphStatistics,
                SectionStatistics,
                DocumentStatistics,
                CorpusStatistics,
                CorpusVocabulary,
                SimilarDocument,
            ],
            "examples.search.schemas.text": [Document, Section, Paragraph, Sentence, Word],
            "examples.search.schemas.search": [
                SearchQuery,
                SentenceSearchResult,
                DocumentSearchTarget,
                SectionSearchTarget,
                ParagraphSearchTarget,
                SentenceSearchTarget,
                DocumentIndexTarget,
                SectionIndexTarget,
                ParagraphIndexTarget,
                SentenceIndexTarget,
                DocumentIndexTerm,
                DocumentIndexSummary,
                SectionIndexTerm,
                SectionIndexSummary,
                ParagraphIndexTerm,
                ParagraphIndexSummary,
                SentenceIndexTerm,
                SentenceIndexSummary,
                DocumentOverlapScore,
                SectionOverlapScore,
                ParagraphOverlapScore,
                SentenceOverlapScore,
                DocumentBm25Score,
                SectionBm25Score,
                ParagraphBm25Score,
                SentenceBm25Score,
                DocumentScore,
                SectionScore,
                ParagraphScore,
                SentenceScore,
                DocumentSearchCandidate,
                DocumentSearchResult,
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
                Label,
                QueryLabel,
                LabelMapEntry,
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
            "examples.search.schemas.relevance": [
                RelevancePolicy,
                QueryDocumentSignals,
                DocumentPopularity,
            ],
            "examples.search.schemas.similarity": [
                SimilarityPolicy,
                SimilarityDocumentQuery,
                SimilaritySectionQuery,
                SimilarityParagraphQuery,
                SimilaritySentenceQuery,
                DocumentSimilarityQuery,
                SectionSimilarityQuery,
                ParagraphSimilarityQuery,
                SentenceSimilarityQuery,
                DocumentSimilarity,
                IndexedSimilarDocument,
                IndexedSimilarSection,
                IndexedSimilarParagraph,
                IndexedSimilarSentence,
                SectionSimilarity,
                ParagraphSimilarity,
                SentenceSimilarity,
            ],
        }
        transforms = (
            (ExtractText, "examples.search.transforms.extract.ExtractText"),
            (ProfileDocuments, "examples.search.transforms.profile.ProfileDocuments"),
            (AnalyzeText, "examples.search.transforms.analyze.AnalyzeText"),
            (CorpusText, "examples.search.transforms.corpus.CorpusText"),
            (CreateIndex, "examples.search.transforms.index.CreateIndex"),
            (
                CreateSimilarityQueries,
                "examples.search.transforms.similarities.CreateSimilarityQueries.CreateSimilarityQueries",
            ),
            (ScoreAll, "examples.search.transforms.scoring.ScoreAll.ScoreAll"),
            (
                ReduceSimilarityScores,
                "examples.search.transforms.similarities.ReduceSimilarityScores.ReduceSimilarityScores",
            ),
            (Similarity, "examples.search.transforms.similarity.Similarity"),
            (SimilarSections, "examples.search.transforms.similarities.SimilarSections.SimilarSections"),
            (SimilarParagraphs, "examples.search.transforms.similarities.SimilarParagraphs.SimilarParagraphs"),
            (SimilarSentences, "examples.search.transforms.similarities.SimilarSentences.SimilarSentences"),
            (AddScores, "examples.search.transforms.score.AddScores"),
            (MergeQueryLabels, "examples.search.transforms.labeling.merge_query_labels.MergeQueryLabels"),
            (SelectExperimentScores, "examples.search.transforms.experiments.select_experiment_scores.SelectExperimentScores"),
            (SearchSentences, "examples.search.transforms.search.SearchSentences"),
            (Impressions, "examples.search.transforms.clicks.Impressions.Impressions"),
            (Clicks, "examples.search.transforms.clicks.Clicks.Clicks"),
            (BuildRelevanceSignals, "examples.search.transforms.relevance.BuildRelevanceSignals"),
            (SearchDocuments, "examples.search.transforms.search.SearchDocuments"),
            (
                EvaluateExperimentDocumentRankingQuality,
                "examples.search.transforms.experiments.search_docs.judged_quality.eval_doc_ranking_quality.EvaluateDocumentRankingQuality",
            ),
            (
                EvaluateExperimentDocumentSearchBehavior,
                "examples.search.transforms.experiments.search_docs.behavior.eval_doc_search_behavior.EvaluateDocumentSearchBehavior",
            ),
            (
                EvaluateDocumentRankingQuality,
                "examples.search.transforms.evaluation.search_docs.judged_quality.EvaluateDocumentRankingQuality.EvaluateDocumentRankingQuality",
            ),
            (
                EvaluateDocumentSearchBehavior,
                "examples.search.transforms.evaluation.search_docs.behavior.EvaluateDocumentSearchBehavior.EvaluateDocumentSearchBehavior",
            ),
            (
                EvaluateLabeledDocumentRankingQuality,
                "examples.search.transforms.evaluation.with_labels.search_docs.judged_quality.eval_doc_ranking_quality.EvaluateDocumentRankingQuality",
            ),
            (
                EvaluateLabeledDocumentSearchBehavior,
                "examples.search.transforms.evaluation.with_labels.search_docs.behavior.eval_doc_search_behavior.EvaluateDocumentSearchBehavior",
            ),
        )
        files = {}
        for transform_class, source_transform in transforms:
            files.update(
                PySpark.render.project()(
                    cast(
                        PySparkExecutionPlan,
                        Compiler.frontend.compile()(transform_class, materialize_schemas=False).lowered,
                    ),
                    source_transform=source_transform,
                    generated_package="examples.structure_generated.search",
                    source_schema_modules=schema_modules,
                )
            )
        documented_schema_modules = {
            module: tuple(
                schema
                for schema in schemas
                if schema
                not in {
                    BehaviorDailyCounts,
                    BehaviorExposure,
                    BehaviorImpression,
                    BehaviorRequest,
                    BehaviorRequestMetrics,
                    BehaviorRequestTotals,
                    EvaluationIdealDcg,
                    EvaluationJudgment,
                    EvaluationJudgmentTotals,
                    LabelMapEntry,
                    QueryLabelAssignmentEntries,
                    QueryLabelAssignments,
                    EvaluationQuery,
                    EvaluationResult,
                    EvaluationResultTotals,
                    TimeWindow,
                }
            )
            for module, schemas in schema_modules.items()
        }
        docs = Docs.render.project()(
            StructureConfig.resolve(
                project_root=ROOT,
                source_roots=["examples"],
                generated_dir="examples/structure_generated/search",
                generated_package="examples.structure_generated.search",
            ),
            DiscoveredStructureProject(
                transforms=tuple(transform for transform, _ in transforms),
                schema_modules=documented_schema_modules,
            ),
        )
        files.update({f"examples/structure_generated/search/{path}": text for path, text in docs.items()})
        files["examples/structure_generated/search/traceability/__init__.py"] = (
            "# Generated traceability package marker.\n"
        )
        files["examples/structure_generated/search/traceability/transforms/__init__.py"] = (
            "# Generated transform traceability package marker.\n"
        )
        return {path: text.rstrip() + "\n" for path, text in files.items()}


def expected_search_generated() -> dict[str, str]:
    return _expected_generated("search")


def render_school_iterable_example() -> dict[str, str]:
    plugin = str(ROOT / "examples/plugins/iterable/src")
    sys.path.insert(0, plugin)
    try:
        from structure.plugin.api.v1 import GenerationRequest

        authoring = importlib.import_module("structure_iterable.authoring.Authoring")
        compiler = importlib.import_module("structure_iterable.compiler.Compiler")
        generation = importlib.import_module("structure_iterable.generation.Generation")
        schemas = importlib.import_module("examples.school.schemas.sequences")

        scores = compiler.IterableRecipe(
            name="ProjectIterableScores",
            inputs=("students", "profiles", "awards"),
            outputs=("reports", "audits"),
            steps=(
                compiler.IterableStep(
                    name="project_scores",
                    inputs=("students", "profiles", "awards"),
                    results=("reports", "audits"),
                    body=authoring.IterableStepBody(
                        joins=(
                            authoring.Join("left", "profiles", authoring.Field("students", "student"), authoring.Field("profiles", "student")),
                            authoring.Join("left", "awards", authoring.Field("students", "student"), authoring.Field("awards", "student")),
                        ),
                        projections=(
                            authoring.Projection(
                                schemas.StudentReport,
                                {
                                    "student": authoring.Field("students", "student"),
                                    "score": authoring.Field("students", "score"),
                                    "cohort": authoring.Field("profiles", "cohort"),
                                    "award": authoring.Field("awards", "award"),
                                },
                            ),
                            authoring.Projection(
                                schemas.StudentAudit,
                                {"student": authoring.Field("students", "student"), "score": authoring.Field("students", "score")},
                            ),
                        ),
                    ),
                ),
            ),
        )
        sequences = compiler.IterableRecipe(
            name="Fibonacci",
            inputs=("rows",),
            outputs=("result",),
            steps=(
                compiler.IterableStep(
                    name="generate",
                    inputs=("rows",),
                    results=("result",),
                    body=authoring.IterableStepBody(
                        joins=(),
                        projections=(
                            authoring.Projection(
                                schemas.FibonacciRow,
                                {"index": authoring.Field("rows", "index"), "fibonacci": authoring.StateValue(0)},
                            ),
                        ),
                        scan=authoring.Scan(
                            initial=(0, 1),
                            next=(authoring.StateValue(1), authoring.BinaryStateExpression("add", authoring.StateValue(0), authoring.StateValue(1))),
                        ),
                    ),
                ),
            ),
        )
        files = generation.Generation().generate(
            GenerationRequest(
                payload={"examples.school.transforms.iterable.ProjectIterableScores": scores},
                source_module="examples.school.transforms.iterable",
                generated_package="structure_generated.school",
            )
        ).files
        files = {
            **files,
            **generation.Generation().generate(
                GenerationRequest(
                    payload={"examples.school.transforms.sequences.Fibonacci": sequences},
                    source_module="examples.school.transforms.sequences",
                    generated_package="structure_generated.school",
                )
            ).files,
        }
        return {f"examples/{path}": text for path, text in files.items()}
    finally:
        sys.path.remove(plugin)
        _drop("structure_iterable")


def expected_school_iterable_generated() -> dict[str, str]:
    return _expected_generated("school")


def _expected_generated(example: str) -> dict[str, str]:
    root = ROOT
    return {
        str(path.relative_to(root)).replace("\\", "/"): path.read_text(encoding="utf-8")
        for path in sorted((EXAMPLES / "structure_generated" / example).rglob("*"))
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
    }


@contextmanager
def _example_imports() -> Iterator[None]:
    path = str(ROOT.resolve())
    sys.path.insert(0, path)
    try:
        yield
    finally:
        sys.path.remove(path)
        _drop("examples.store")
        _drop("examples.streams")
        _drop("examples.stocks")
        _drop("examples.search")
        _drop("examples.structure_generated")


def _drop(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            sys.modules.pop(name, None)
