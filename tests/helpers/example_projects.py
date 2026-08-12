from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence, cast

from structure import *
from structure.core.compiler.api import Compiler
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
        from examples.store.schemas.catalog import CatalogAvailability, CatalogProduct
        from examples.store.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
        from examples.store.schemas.customer import Customer
        from examples.store.schemas.evaluation import (
            EvaluationBatch,
            RecommendationBehavior,
            RecommendationVariantMetric,
            RecommendationVariantMetricTotals,
        )
        from examples.store.schemas.experiment import RecommendationAssignment, RecommendationExperiment
        from examples.store.schemas.experiment import RecommendationExposure as RecommendationExperimentExposure
        from examples.store.schemas.fulfillment.analytics.summary import DailyFulfillmentSummary, WarehouseLoadSummary
        from examples.store.schemas.fulfillment.demand import DemandWindow, Order
        from examples.store.schemas.fulfillment.evaluation.service import (
            DailyFulfillmentServiceSummary,
            FulfillmentServiceEvaluation,
            FulfillmentServiceTotals,
        )
        from examples.store.schemas.fulfillment.inventory import InboundInventory, InventoryPosition, LeadTime
        from examples.store.schemas.fulfillment.planning.plan import (
            FulfillmentAllocation,
            FulfillmentBackorder,
            FulfillmentPlan,
            ReplenishmentSuggestion,
        )
        from examples.store.schemas.fulfillment.planning.workflow import (
            FulfillmentOption,
            FulfillmentPreferredOption,
            InboundInventoryAvailability,
        )
        from examples.store.schemas.fulfillment.projections.projection import InventoryProjection
        from examples.store.schemas.fulfillment.reconciliation.reconciliation import FulfillmentReconciliation
        from examples.store.schemas.fulfillment.shortages.exception import FulfillmentException, ServiceRiskTarget
        from examples.store.schemas.fulfillment.shortages.shortage import FulfillmentShortage, FulfillmentShortageRanked
        from examples.store.schemas.fulfillment.substitutions.substitution import (
            FulfillmentSubstitutionOption,
            SubstitutionRule,
        )
        from examples.store.schemas.fulfillment.warehouses import Warehouse
        from examples.store.schemas.merchandising import (
            DailyRecommendationBehavior,
            DailyRecommendationClicks,
            DailyRecommendationCounts,
            DailyRecommendationImpressions,
            DiversificationDecision,
            DiversifiedRecommendationCandidate,
            MerchandisingBoost,
            MerchandisingPolicy,
            MerchandisingSuppression,
            ProductRecommendationSignal,
            ProductRecommendationSignalTotals,
            RankedRecommendationCandidate,
            RecommendationBehaviorImpression,
            RecommendationCandidate,
            RecommendationCandidateDecision,
            RecommendationClick,
            RecommendationClickSummary,
            RecommendationEvaluationBatch,
            RecommendationExposure,
            RecommendationImpression,
            RecommendationPurchase,
            RecommendationRequest,
            RecommendationRequestBehavior,
            RecommendationRun,
            RecommendedProduct,
            SessionEvent,
            SessionFeature,
        )
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
        from examples.store.schemas.personalization import (
            PersonalizationHistory,
            PersonalizedRecommendation,
            UserFeaturePreference,
        )
        from examples.store.schemas.product import BlockedProduct, Product, ProductBase
        from examples.store.schemas.promotion import Promotion
        from examples.store.schemas.shipment import Shipment
        from examples.store.schemas.taxonomy import (
            ExpandedProductTaxonomy,
            ProductTaxonomy,
            TaxonomyAncestor,
            TaxonomyNode,
        )
        from examples.store.transforms.adv_analytics import AdvancedOrderAnalytics
        from examples.store.transforms.analytics import FulfillmentAnalytics, OrderAnalytics
        from examples.store.transforms.catalog.prepare import PrepareCatalog
        from examples.store.transforms.evaluation.fulfillment.service import EvaluateFulfillment
        from examples.store.transforms.evaluation.recommender.behavior.workflow import EvaluateRecommendations
        from examples.store.transforms.experiments.assign import AssignRecommendationVariants
        from examples.store.transforms.experiments.evaluation.recommendations.experiment import (
            EvaluateRecommendationExperiment,
        )
        from examples.store.transforms.experiments.exposure import RecordRecommendationExposures
        from examples.store.transforms.experiments.select_active import SelectActiveRecommendationExperiments
        from examples.store.transforms.fulfillment.demand.prepare import PrepareOrderDemand
        from examples.store.transforms.fulfillment.demand.windows import BuildDemandWindows
        from examples.store.transforms.fulfillment.inventory.project import ProjectInventory
        from examples.store.transforms.fulfillment.planning.plan import PlanFulfillment
        from examples.store.transforms.fulfillment.reconciliation.reconcile import ReconcileFulfillmentPlan
        from examples.store.transforms.fulfillment.shortages.detect import DetectShortages
        from examples.store.transforms.fulfillment.shortages.exceptions import PrioritizeExceptions
        from examples.store.transforms.fulfillment.substitutions.find_substitutions import FindSubstitutions
        from examples.store.transforms.fulfillment.workflow import Fulfillment
        from examples.store.transforms.merchandising.workflow import Merchandising
        from examples.store.transforms.orders.enrich import EnrichOrders
        from examples.store.transforms.personalization.features import BuildProductFeatures
        from examples.store.transforms.personalization.history import BuildPersonalizationHistory
        from examples.store.transforms.personalization.score import ScorePersonalizedRecommendations
        from examples.store.transforms.personalization.workflow import BuildPersonalizedRecommendations
        from examples.store.transforms.recommender.candidates import BuildRecommendationCandidates
        from examples.store.transforms.recommender.candidates.admit import SelectRecommendationCandidates
        from examples.store.transforms.recommender.candidates.filter import FilterRecommendationCandidates
        from examples.store.transforms.recommender.candidates.generate import GenerateRecommendationCandidates
        from examples.store.transforms.recommender.diversify import DiversifyRecommendations
        from examples.store.transforms.recommender.signals.products import BuildProductSignals
        from examples.store.transforms.recommender.signals.purchases import BuildPurchaseSignals
        from examples.store.transforms.recommender.signals.session import BuildSessionSignals
        from examples.store.transforms.recommender.signals.workflow import BuildRecommendationSignals
        from examples.store.transforms.recommender.workflow import Recommender
        from examples.store.transforms.rowset_joins.rowset_join_examples import RowsetJoinExamples
        from examples.store.transforms.taxonomy.expand_taxonomy import ExpandProductTaxonomy

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
            "examples.store.schemas.fulfillment.analytics.summary": [
                DailyFulfillmentSummary,
                WarehouseLoadSummary,
            ],
            "examples.store.schemas.fulfillment.demand.demand": [Order],
            "examples.store.schemas.fulfillment.demand.windows": [DemandWindow],
            "examples.store.schemas.fulfillment.evaluation.service": [
                FulfillmentServiceTotals,
                FulfillmentServiceEvaluation,
                DailyFulfillmentServiceSummary,
            ],
            "examples.store.schemas.fulfillment.inventory.inventory": [InboundInventory, InventoryPosition, LeadTime],
            "examples.store.schemas.fulfillment.warehouses.warehouse": [Warehouse],
            "examples.store.schemas.fulfillment.planning.workflow": [
                InboundInventoryAvailability,
                FulfillmentOption,
                FulfillmentPreferredOption,
            ],
            "examples.store.schemas.fulfillment.planning.plan": [
                FulfillmentAllocation,
                FulfillmentBackorder,
                FulfillmentPlan,
                ReplenishmentSuggestion,
            ],
            "examples.store.schemas.fulfillment.reconciliation.reconciliation": [FulfillmentReconciliation],
            "examples.store.schemas.fulfillment.projections.projection": [InventoryProjection],
            "examples.store.schemas.fulfillment.shortages.shortage": [FulfillmentShortageRanked, FulfillmentShortage],
            "examples.store.schemas.fulfillment.shortages.exception": [ServiceRiskTarget, FulfillmentException],
            "examples.store.schemas.fulfillment.substitutions.substitution": [
                SubstitutionRule,
                FulfillmentSubstitutionOption,
            ],
            "examples.store.schemas.catalog": [
                CatalogProduct,
                CatalogAvailability,
            ],
            "examples.store.schemas.taxonomy": [
                TaxonomyNode,
                ProductTaxonomy,
                TaxonomyAncestor,
                ExpandedProductTaxonomy,
            ],
            "examples.store.schemas.merchandising.policy": [
                MerchandisingPolicy,
                MerchandisingBoost,
                MerchandisingSuppression,
            ],
            "examples.store.schemas.merchandising.recommendation": [
                RecommendationCandidate,
                RecommendationCandidateDecision,
                RecommendationRequest,
                RecommendedProduct,
                RecommendationRun,
            ],
            "examples.store.schemas.merchandising.feedback": [
                RecommendationImpression,
                RecommendationClick,
                DailyRecommendationImpressions,
                DailyRecommendationClicks,
                ProductRecommendationSignal,
                RecommendationPurchase,
            ],
            "examples.store.schemas.merchandising.session": [SessionEvent, SessionFeature],
            "examples.store.schemas.merchandising.evaluation": [
                RecommendationEvaluationBatch,
                RecommendationRequestBehavior,
                DailyRecommendationBehavior,
            ],
            "examples.store.schemas.merchandising.intermediate": [
                RankedRecommendationCandidate,
                DiversifiedRecommendationCandidate,
                DiversificationDecision,
                RecommendationBehaviorImpression,
                RecommendationClickSummary,
                RecommendationExposure,
                DailyRecommendationCounts,
                ProductRecommendationSignalTotals,
            ],
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
            "examples.store.schemas.personalization": [
                UserFeaturePreference,
                PersonalizationHistory,
                PersonalizedRecommendation,
            ],
            "examples.store.schemas.promotion": [Promotion],
            "examples.store.schemas.shipment": [Shipment],
            "examples.store.schemas.experiment": [
                RecommendationExperiment,
                RecommendationAssignment,
                RecommendationExperimentExposure,
            ],
            "examples.store.schemas.evaluation.batch": [EvaluationBatch],
            "examples.store.schemas.evaluation.recommendations": [
                RecommendationBehavior,
                RecommendationVariantMetric,
                RecommendationVariantMetricTotals,
            ],
        }
        files = {}
        transforms = (
            (EnrichOrders, "examples.store.transforms.orders.enrich.EnrichOrders"),
            (
                PrepareOrderDemand,
                "examples.store.transforms.fulfillment.demand.prepare.PrepareOrderDemand",
            ),
            (
                BuildDemandWindows,
                "examples.store.transforms.fulfillment.demand.windows.BuildDemandWindows",
            ),
            (
                ProjectInventory,
                "examples.store.transforms.fulfillment.inventory.project.ProjectInventory",
            ),
            (
                PlanFulfillment,
                "examples.store.transforms.fulfillment.planning.plan.PlanFulfillment",
            ),
            (
                DetectShortages,
                "examples.store.transforms.fulfillment.shortages.detect.DetectShortages",
            ),
            (
                FindSubstitutions,
                "examples.store.transforms.fulfillment.substitutions.find_substitutions.FindSubstitutions",
            ),
            (
                PrioritizeExceptions,
                "examples.store.transforms.fulfillment.shortages.exceptions.PrioritizeExceptions",
            ),
            (
                ReconcileFulfillmentPlan,
                "examples.store.transforms.fulfillment.reconciliation.reconcile.ReconcileFulfillmentPlan",
            ),
            (
                EvaluateFulfillment,
                "examples.store.transforms.evaluation.fulfillment.service.EvaluateFulfillment",
            ),
            (
                FulfillmentAnalytics,
                "examples.store.transforms.analytics.fulfillment.analytics.FulfillmentAnalytics",
            ),
            (Fulfillment, "examples.store.transforms.fulfillment.workflow.Fulfillment"),
            (PrepareCatalog, "examples.store.transforms.catalog.prepare_catalog.PrepareCatalog"),
            (
                ExpandProductTaxonomy,
                "examples.store.transforms.taxonomy.expand_taxonomy.ExpandProductTaxonomy",
            ),
            (
                SelectRecommendationCandidates,
                "examples.store.transforms.recommender.candidates.admit.SelectRecommendationCandidates",
            ),
            (
                BuildRecommendationCandidates,
                "examples.store.transforms.recommender.candidates.workflow.BuildRecommendationCandidates",
            ),
            (
                Recommender,
                "examples.store.transforms.recommender.workflow.Recommender",
            ),
            (
                GenerateRecommendationCandidates,
                "examples.store.transforms.recommender.candidates.generate.GenerateRecommendationCandidates",
            ),
            (
                FilterRecommendationCandidates,
                "examples.store.transforms.recommender.candidates.filter.FilterRecommendationCandidates",
            ),
            (
                DiversifyRecommendations,
                "examples.store.transforms.recommender.diversify.DiversifyRecommendations",
            ),
            (Merchandising, "examples.store.transforms.merchandising.workflow.Merchandising"),
            (
                BuildProductFeatures,
                "examples.store.transforms.personalization.features.BuildProductFeatures",
            ),
            (
                BuildPersonalizationHistory,
                "examples.store.transforms.personalization.history.BuildPersonalizationHistory",
            ),
            (
                ScorePersonalizedRecommendations,
                "examples.store.transforms.personalization.score.ScorePersonalizedRecommendations",
            ),
            (
                BuildPersonalizedRecommendations,
                "examples.store.transforms.personalization.workflow.BuildPersonalizedRecommendations",
            ),
            (
                BuildProductSignals,
                "examples.store.transforms.recommender.signals.products.BuildProductSignals",
            ),
            (
                BuildSessionSignals,
                "examples.store.transforms.recommender.signals.session.BuildSessionSignals",
            ),
            (
                BuildPurchaseSignals,
                "examples.store.transforms.recommender.signals.purchases.BuildPurchaseSignals",
            ),
            (
                BuildRecommendationSignals,
                "examples.store.transforms.recommender.signals.workflow.BuildRecommendationSignals",
            ),
            (
                EvaluateRecommendations,
                "examples.store.transforms.evaluation.recommender.behavior.workflow.EvaluateRecommendations",
            ),
            (RowsetJoinExamples, "examples.store.transforms.rowset_joins.rowset_join_examples.RowsetJoinExamples"),
            (
                SelectActiveRecommendationExperiments,
                "examples.store.transforms.experiments.select_active.SelectActiveRecommendationExperiments",
            ),
            (
                AssignRecommendationVariants,
                "examples.store.transforms.experiments.assign.AssignRecommendationVariants",
            ),
            (
                RecordRecommendationExposures,
                "examples.store.transforms.experiments.exposure.RecordRecommendationExposures",
            ),
            (
                EvaluateRecommendationExperiment,
                "examples.store.transforms.experiments.evaluation.recommendations.experiment.EvaluateRecommendationExperiment",
            ),
            (OrderAnalytics, "examples.store.transforms.analytics.orders.workflow.OrderAnalytics"),
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
        return {path: text.rstrip() + "\n" for path, text in files.items()}


def expected_stocks_generated() -> dict[str, str]:
    return _expected_generated("stocks")


def render_security_example() -> dict[str, str]:
    with _example_imports():
        from examples.security.schemas.alarms import TeamVulnerabilityAlarm
        from examples.security.schemas.assets import OS, App, Device, DeviceType, Scanner, Software
        from examples.security.schemas.events import AppEvent, RawEvent, VulnEvent
        from examples.security.schemas.notifications import PersonVulnerabilityNotification
        from examples.security.schemas.organization import Department, Org, Person, Team
        from examples.security.schemas.remediation import (
            DepartmentRemediationWorkflowSummary,
            ExpiredExceptionVulnerability,
            ExpiringExceptionVulnerability,
            OrgRemediationWorkflowSummary,
            PendingExceptionVulnerability,
            PersonRemediationWorkflowSummary,
            RemediationCase,
            RemediationCaseAggregate,
            RemediationCaseCheck,
            RemediationCaseIssue,
            RemediationWorkflowActivity,
            RemediationWorkflowSummary,
            TeamRemediationWorkflowSummary,
            UnacknowledgedVulnerability,
            VulnerabilityWorkflowExposure,
        )
        from examples.security.schemas.reporting import (
            AppAuditEvent,
            DeliveryReceipt,
            DepartmentActiveVulnerability,
            DepartmentVulnerabilityDeadlineSummary,
            DepartmentVulnerabilityStatistic,
            DeviceActiveVulnerability,
            OrgActiveVulnerability,
            OrgVulnerabilityDeadlineSummary,
            OrgVulnerabilityStatistic,
            PersonActiveVulnerability,
            PersonVulnerabilityDeadlineSummary,
            PersonVulnerabilityStatistic,
            ReportingPeriod,
            SecurityEvaluation,
            TeamActiveVulnerability,
            TeamVulnerabilityDeadlineSummary,
            TeamVulnerabilityStatistic,
            VulnerabilityAuditEvent,
            VulnerabilityDeadlineActivity,
            VulnerabilityDeadlineSummary,
            VulnerabilityDiscovery,
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
        from examples.security.schemas.risk import RemediationPolicy, Vuln, VulnType
        from examples.security.transforms.alarms import VulnerabilityAlarms
        from examples.security.transforms.deadlines import VulnerabilityDeadlineReports
        from examples.security.transforms.events import EnrichAppEvents, EnrichVulnerabilityEvents
        from examples.security.transforms.notify import VulnerabilityNotifications
        from examples.security.transforms.posture import SecurityPosture
        from examples.security.transforms.quality import SecurityInventoryQuality
        from examples.security.transforms.remediate.workflow import VulnerabilityRemediationWorkflow
        from examples.security.transforms.reports import ActiveVulnerabilityReports, VulnerabilityStatistics

        schema_modules: dict[str, Sequence[type[Schema]]] = {
            "examples.security.schemas.assets": [DeviceType, Software, App, OS, Scanner, Device],
            "examples.security.schemas.events": [RawEvent, AppEvent, VulnEvent],
            "examples.security.schemas.organization": [Org, Department, Team, Person],
            "examples.security.schemas.reporting": [
                ReportingPeriod,
                SecurityEvaluation,
                DeliveryReceipt,
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
                VulnerabilityDeadlineSummary,
                PersonVulnerabilityDeadlineSummary,
                TeamVulnerabilityDeadlineSummary,
                DepartmentVulnerabilityDeadlineSummary,
                OrgVulnerabilityDeadlineSummary,
                VulnerabilityLifecycle,
                VulnerabilityDiscovery,
                VulnerabilityDeadlineActivity,
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
            "examples.security.schemas.remediate": [
                RemediationCase,
                RemediationCaseAggregate,
                RemediationCaseCheck,
                RemediationCaseIssue,
                VulnerabilityWorkflowExposure,
                UnacknowledgedVulnerability,
                PendingExceptionVulnerability,
                ExpiringExceptionVulnerability,
                ExpiredExceptionVulnerability,
                RemediationWorkflowActivity,
                RemediationWorkflowSummary,
                PersonRemediationWorkflowSummary,
                TeamRemediationWorkflowSummary,
                DepartmentRemediationWorkflowSummary,
                OrgRemediationWorkflowSummary,
            ],
            "examples.security.schemas.notifications": [PersonVulnerabilityNotification],
            "examples.security.schemas.alarms": [TeamVulnerabilityAlarm],
            "examples.security.schemas.risk": [VulnType, RemediationPolicy, Vuln],
        }
        transforms = (
            (EnrichAppEvents, "examples.security.transforms.events.EnrichAppEvents"),
            (EnrichVulnerabilityEvents, "examples.security.transforms.events.EnrichVulnerabilityEvents"),
            (SecurityPosture, "examples.security.transforms.posture.SecurityPosture"),
            (
                VulnerabilityRemediationWorkflow,
                "examples.security.transforms.remediate.workflow.VulnerabilityRemediationWorkflow",
            ),
            (VulnerabilityNotifications, "examples.security.transforms.notify.VulnerabilityNotifications"),
            (VulnerabilityAlarms, "examples.security.transforms.alarms.VulnerabilityAlarms"),
            (VulnerabilityDeadlineReports, "examples.security.transforms.deadlines.VulnerabilityDeadlineReports"),
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
        return {path: text.rstrip() + "\n" for path, text in files.items()}


def expected_security_generated() -> dict[str, str]:
    return _expected_generated("security")


def render_search_example() -> dict[str, str]:
    with _example_imports():
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
            ParagraphVectorCandidate,
            ParagraphVectorEmbedding,
            ParagraphVectorIndex,
            ParagraphVectorIndexSummary,
            ParagraphVectorQuery,
            ParagraphVectorScore,
            SearchQueryVectorEmbedding,
            SimilarityDocumentVectorEmbedding,
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
            QueryTerm,
            QueryTermCount,
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
            ParagraphScore,
            ParagraphSearchTarget,
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
        from examples.search.schemas.similarities.vector import (
            DocumentFusedSimilarityCandidate,
            ParagraphFusedSimilarityCandidate,
        )
        from examples.search.schemas.similarity import (
            DocumentSimilarity,
            DocumentSimilarityQuery,
            HybridIndexedSimilarDocument,
            HybridIndexedSimilarParagraph,
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
            SimilarityFusionPolicy,
            SimilarityParagraphQuery,
            SimilarityPolicy,
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
        from examples.search.transforms.experiment import (
            EvaluateDocSearchBehavior as EvaluateExperimentDocSearchBehavior,
        )
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
        from examples.search.transforms.scoring.lexical.ScoreBm25 import ScoreBm25
        from examples.search.transforms.scoring.lexical.ScoreOverlap import ScoreOverlap
        from examples.search.transforms.scoring.lexical.SelectPopularQueries import SelectPopularQueries
        from examples.search.transforms.scoring.lexical.SelectRecentQueries import SelectRecentQueries
        from examples.search.transforms.search import SearchDocuments, SearchFields, SearchSentences
        from examples.search.transforms.searching.search_similarity import SearchSimilarity, SearchSimilarityParagraphs
        from examples.search.transforms.similarities.CreateSimilarityQueries import CreateSimilarityQueries
        from examples.search.transforms.similarities.ReduceSimilarityScores import ReduceSimilarityScores
        from examples.search.transforms.similarities.Similarities import Similarities
        from examples.search.transforms.similarities.SimilarParagraphs import SimilarParagraphs
        from examples.search.transforms.similarities.SimilarSections import SimilarSections
        from examples.search.transforms.similarities.SimilarSentences import SimilarSentences
        from examples.search.transforms.stats.AnalyzeText import AnalyzeText
        from examples.search.transforms.stats.CorpusText import CorpusText
        from examples.search.transforms.stats.ProfileDocuments import ProfileDocuments
        from examples.search.transforms.training import BuildTrainingData, RankDocumentCandidates
        from structure.plugin.pyspark import TimeWindow

        schema_modules: dict[str, Sequence[type[Schema]]] = {
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
            "examples.search.schemas.search": [
                SearchQuery,
                SentenceSearchResult,
                DocumentSearchTarget,
                SectionSearchTarget,
                ParagraphSearchTarget,
                SentenceSearchTarget,
                ScorePolicy,
                QueryPopularity,
                DocumentScore,
                SectionScore,
                ParagraphScore,
                SentenceScore,
                DocumentFeedbackOption,
                QueryDocumentFeedback,
                PopularityFeedback,
                DocumentSearchCandidate,
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
                QueryTerm,
                QueryTermCount,
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
                SimilarityDocumentQuery,
                SimilaritySectionQuery,
                SimilarityParagraphQuery,
                SimilaritySentenceQuery,
                DocumentSimilarityQuery,
                SectionSimilarityQuery,
                ParagraphSimilarityQuery,
                SentenceSimilarityQuery,
                DocumentSimilarity,
                HybridIndexedSimilarDocument,
                HybridIndexedSimilarParagraph,
                IndexedSimilarDocument,
                IndexedSimilarSection,
                IndexedSimilarParagraph,
                IndexedSimilarSentence,
                SectionSimilarity,
                ParagraphSimilarity,
                SentenceSimilarity,
            ],
            "examples.search.schemas.indexing.vector": [
                DocumentVectorEmbedding,
                ParagraphVectorEmbedding,
                DocumentVectorQuery,
                SearchQueryVectorEmbedding,
                SimilarityDocumentVectorEmbedding,
                ParagraphVectorQuery,
                DocumentVectorIndex,
                ParagraphVectorIndex,
                DocumentVectorIndexSummary,
                ParagraphVectorIndexSummary,
                DocumentVectorScore,
                DocumentVectorCandidate,
                ParagraphVectorScore,
                ParagraphVectorCandidate,
                VectorIndexPolicy,
            ],
            "examples.search.schemas.similarities.vector": [
                DocumentFusedSimilarityCandidate,
                ParagraphFusedSimilarityCandidate,
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
        }
        transforms = (
            (All, "examples.search.transforms.all.all.All"),
            (Chunking, "examples.search.transforms.chunking.Chunking.Chunking"),
            (DocumentChunking, "examples.search.transforms.chunking.DocumentChunking.DocumentChunking"),
            (SentenceChunking, "examples.search.transforms.chunking.SentenceChunking.SentenceChunking"),
            (ProfileDocuments, "examples.search.transforms.stats.ProfileDocuments.ProfileDocuments"),
            (BuildDocumentFeatures, "examples.search.transforms.features.BuildDocumentFeatures.BuildDocumentFeatures"),
            (BuildQueryFeatures, "examples.search.transforms.features.BuildQueryFeatures.BuildQueryFeatures"),
            (Features, "examples.search.transforms.features.Features.Features"),
            (BuildTrainingData, "examples.search.transforms.training.BuildTrainingData.BuildTrainingData"),
            (
                RankDocumentCandidates,
                "examples.search.transforms.training.RankDocumentCandidates.RankDocumentCandidates",
            ),
            (Training, "examples.search.transforms.training.Training.Training"),
            (AnalyzeText, "examples.search.transforms.stats.AnalyzeText.AnalyzeText"),
            (CorpusText, "examples.search.transforms.stats.CorpusText.CorpusText"),
            (Indexing, "examples.search.transforms.indexing.Indexing.Indexing"),
            (ExtractDocumentFields, "examples.search.transforms.fields.ExtractDocumentFields.ExtractDocumentFields"),
            (FieldIndex, "examples.search.transforms.indexing.fields.FieldIndex.FieldIndex"),
            (SearchFields, "examples.search.transforms.searching.search_fields.SearchFields.SearchFields"),
            (
                CreateSimilarityQueries,
                "examples.search.transforms.similarities.CreateSimilarityQueries.CreateSimilarityQueries",
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
                "examples.search.transforms.similarities.ReduceSimilarityScores.ReduceSimilarityScores",
            ),
            (Similarities, "examples.search.transforms.similarities.Similarities.Similarities"),
            (
                SearchSimilarity,
                "examples.search.transforms.searching.search_similarity.SearchSimilarity.SearchSimilarity",
            ),
            (
                SearchSimilarityParagraphs,
                "examples.search.transforms.searching.search_similarity.SearchSimilarityParagraphs.SearchSimilarityParagraphs",
            ),
            (SimilarSections, "examples.search.transforms.similarities.SimilarSections.SimilarSections"),
            (SimilarParagraphs, "examples.search.transforms.similarities.SimilarParagraphs.SimilarParagraphs"),
            (SimilarSentences, "examples.search.transforms.similarities.SimilarSentences.SimilarSentences"),
            (ResolveCohortBands, "examples.search.transforms.cohorts.ResolveCohortBands.ResolveCohortBands"),
            (MergeQueryLabels, "examples.search.transforms.labeling.MergeQueryLabels.MergeQueryLabels"),
            (CreateQueryLabels, "examples.search.transforms.labeling.CreateQueryLabels.CreateQueryLabels"),
            (Labeling, "examples.search.transforms.labeling.Labeling.Labeling"),
            (
                SelectExperimentScores,
                "examples.search.transforms.experiments.SelectExperimentScores.SelectExperimentScores",
            ),
            (SearchSentences, "examples.search.transforms.search.SearchSentences"),
            (Impressions, "examples.search.transforms.clicks.Impressions.Impressions"),
            (Clicks, "examples.search.transforms.clicks.Clicks.Clicks"),
            (
                BuildRelevanceSignals,
                "examples.search.transforms.relevance.BuildRelevanceSignals.BuildRelevanceSignals",
            ),
            (SearchDocuments, "examples.search.transforms.search.SearchDocuments"),
            (
                Searching001AdjustRerankSearchDocuments,
                "examples.search.transforms.experiments.searching.search_docs.Searching001AdjustRerankSearchDocuments.Searching001AdjustRerankSearchDocuments",
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
        )
        files = {}
        for transform_class, source_transform in transforms:
            files.update(
                PySpark.render.project()(
                    cast(
                        PySparkExecutionPlan,
                        Compiler.frontend.compile()(
                            transform_class,
                            materialize_schemas=False,
                            project_root=EXAMPLES / "search",
                        ).lowered,
                    ),
                    source_transform=source_transform,
                    generated_package="examples.structure_generated.search",
                    source_schema_modules=schema_modules,
                    generated_code_options=("embed_udfs",),
                )
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
        schemas = importlib.import_module("examples.school.schemas.iterable")

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
                            authoring.Join(
                                "left",
                                "profiles",
                                authoring.Field("students", "student"),
                                authoring.Field("profiles", "student"),
                            ),
                            authoring.Join(
                                "left",
                                "awards",
                                authoring.Field("students", "student"),
                                authoring.Field("awards", "student"),
                            ),
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
                                {
                                    "student": authoring.Field("students", "student"),
                                    "score": authoring.Field("students", "score"),
                                },
                            ),
                        ),
                    ),
                ),
            ),
        )
        fibonacci = compiler.IterableRecipe(
            name="IterableFibonacci",
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
                                schemas.IterableFibonacciRow,
                                {"index": authoring.Field("rows", "index"), "fibonacci": authoring.StateValue(0)},
                            ),
                        ),
                        scan=authoring.Scan(
                            initial=(0, 1),
                            next=(
                                authoring.StateValue(1),
                                authoring.BinaryStateExpression(
                                    "add", authoring.StateValue(0), authoring.StateValue(1)
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        files = (
            generation.Generation()
            .generate(
                GenerationRequest(
                    payload={
                        "examples.school.transforms.iterable.ProjectIterableScores": scores,
                        "examples.school.transforms.iterable.IterableFibonacci": fibonacci,
                    },
                    source_module="examples.school.transforms.iterable",
                    generated_package="structure_generated.school",
                )
            )
            .files
        )
        return {**{f"examples/{path}": text for path, text in files.items()}, **_render_school_pyspark_example()}
    finally:
        sys.path.remove(plugin)
        _drop("structure_iterable")


def _render_school_pyspark_example() -> dict[str, str]:
    from examples.school.schemas.sequences import (
        FibonacciNumber,
        FibonacciState,
        PrimeNumber,
        PrimeState,
        SeriesApproximation,
        SeriesState,
        Tick,
    )
    from examples.school.transforms.sequences import Fibonacci, PrimeNumbers
    from examples.school.transforms.series import EAsSeries, Ln2AsSeries, PiAsSeries

    schema_modules: dict[str, Sequence[type[Schema]]] = {
        "examples.school.schemas.sequences": [
            Tick,
            FibonacciState,
            FibonacciNumber,
            PrimeState,
            PrimeNumber,
            SeriesState,
            SeriesApproximation,
        ]
    }
    files = PySpark.render.project().source_unit(
        {
            "examples.school.transforms.sequences.Fibonacci": cast(
                PySparkExecutionPlan,
                Compiler.frontend.compile()(Fibonacci, materialize_schemas=False).lowered,
            ),
            "examples.school.transforms.sequences.PrimeNumbers": cast(
                PySparkExecutionPlan,
                Compiler.frontend.compile()(PrimeNumbers, materialize_schemas=False).lowered,
            ),
        },
        source_module="examples.school.transforms.sequences",
        generated_package="examples.structure_generated.school",
        source_schema_modules=schema_modules,
    )
    files.update(
        PySpark.render.project().source_unit(
            {
                f"examples.school.transforms.series.{transform.__name__}": cast(
                    PySparkExecutionPlan,
                    Compiler.frontend.compile()(transform, materialize_schemas=False).lowered,
                )
                for transform in (PiAsSeries, EAsSeries, Ln2AsSeries)
            },
            source_module="examples.school.transforms.series",
            generated_package="examples.structure_generated.school",
            source_schema_modules=schema_modules,
        )
    )
    return files


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
