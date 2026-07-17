from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

from structure import *
from structure.app.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.app.configuration.model.StructureConfig import StructureConfig
from structure.app.docs.api import Docs
from structure.app.dsl.model.schemas.Schema import Schema
from structure.app.target.capabilities.api import Capabilities
from structure.app.target.pyspark.api import PySpark

ROOT = Path(".")
EXAMPLES = ROOT / "examples"


def render_orders_example() -> dict[str, str]:
    with _example_imports():
        from examples.orders.schemas.adv_analytics import (
            OrderCollectionProfile,
            OrderCollectionSource,
            OrderCustomerWindow,
            OrderProductCube,
            OrderRevenueRollup,
        )
        from examples.orders.schemas.analytics import CustomerDailyTotal, CustomerEventRank, ProductDailySummary
        from examples.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
        from examples.orders.schemas.customer import Customer
        from examples.orders.schemas.order import (
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
        from examples.orders.schemas.product import BlockedProduct, Product, ProductBase
        from examples.orders.schemas.promotion import Promotion
        from examples.orders.schemas.shipment import Shipment
        from examples.orders.schemas.v3 import V3OrderDetails, V3OrderProjection, V3OrderSource
        from examples.orders.transforms.adv_analytics import AdvancedOrderAnalytics
        from examples.orders.transforms.analytics import OrderAnalytics
        from examples.orders.transforms.order import EnrichOrders
        from examples.orders.transforms.rowset_join import RowsetJoinExamples
        from examples.orders.transforms.v3 import V3OrderFeatures

        schema_modules: dict[str, Sequence[type[Schema]]] = {
            "examples.orders.schemas.adv_analytics": [
                OrderRevenueRollup,
                OrderProductCube,
                OrderCustomerWindow,
                OrderCollectionSource,
                OrderCollectionProfile,
            ],
            "examples.orders.schemas.analytics": [CustomerDailyTotal, ProductDailySummary, CustomerEventRank],
            "examples.orders.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
            "examples.orders.schemas.customer": [Customer],
            "examples.orders.schemas.order": [
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
            "examples.orders.schemas.product": [ProductBase, Product, BlockedProduct],
            "examples.orders.schemas.promotion": [Promotion],
            "examples.orders.schemas.shipment": [Shipment],
            "examples.orders.schemas.v3": [V3OrderDetails, V3OrderSource, V3OrderProjection],
        }
        files = {}
        transforms = (
            (EnrichOrders, "examples.orders.transforms.order.EnrichOrders"),
            (RowsetJoinExamples, "examples.orders.transforms.rowset_join.RowsetJoinExamples"),
            (OrderAnalytics, "examples.orders.transforms.analytics.OrderAnalytics"),
            (AdvancedOrderAnalytics, "examples.orders.transforms.adv_analytics.AdvancedOrderAnalytics"),
            (V3OrderFeatures, "examples.orders.transforms.v3.V3OrderFeatures"),
        )
        for transform_class, source_transform in transforms:
            capabilities = (
                Capabilities.resolve()(target_profile=">=4.0,<4.1") if transform_class is V3OrderFeatures else None
            )
            files.update(
                PySpark.render.project()(
                    PySpark.plan.lower()(compile_transform(transform_class), capabilities=capabilities),
                    source_transform=source_transform,
                    generated_package="examples.structure_generated.orders",
                    source_schema_modules=schema_modules,
                )
            )
        docs = Docs.render.project()(
            StructureConfig.resolve(
                project_root=ROOT,
                source_roots=["examples"],
                generated_dir="examples/structure_generated/orders",
                generated_package="examples.structure_generated.orders",
            ),
            DiscoveredStructureProject(
                transforms=tuple(transform for transform, _ in transforms),
                schema_modules={module: tuple(schemas) for module, schemas in schema_modules.items()},
            ),
        )
        files.update({f"examples/structure_generated/orders/{path}": text for path, text in docs.items()})
        files["examples/structure_generated/orders/traceability/__init__.py"] = (
            "# Generated traceability package marker.\n"
        )
        files["examples/structure_generated/orders/traceability/transforms/__init__.py"] = (
            "# Generated transform traceability package marker.\n"
        )
        return files


def expected_orders_generated() -> dict[str, str]:
    return _expected_generated("orders")


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
                    PySpark.plan.lower()(compile_transform(transform_class)),
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
                    PySpark.plan.lower()(compile_transform(transform_class)),
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


def render_texts_example() -> dict[str, str]:
    with _example_imports():
        from examples.texts.schemas.analytics import (
            CorpusStatistics,
            CorpusVocabulary,
            DocumentFeatures,
            DocumentStatistics,
            ParagraphStatistics,
            SectionStatistics,
            SentenceStatistics,
            SimilarDocument,
        )
        from examples.texts.schemas.text import Document, Paragraph, Section, Sentence, Word
        from examples.texts.transforms.analyze import AnalyzeText
        from examples.texts.transforms.corpus import CorpusText
        from examples.texts.transforms.extract import ExtractText
        from examples.texts.transforms.profile import ProfileDocuments

        schema_modules: dict[str, Sequence[type[Schema]]] = {
            "examples.texts.schemas.analytics": [
                DocumentFeatures,
                SentenceStatistics,
                ParagraphStatistics,
                SectionStatistics,
                DocumentStatistics,
                CorpusStatistics,
                CorpusVocabulary,
                SimilarDocument,
            ],
            "examples.texts.schemas.text": [Document, Section, Paragraph, Sentence, Word],
        }
        transforms = (
            (ExtractText, "examples.texts.transforms.extract.ExtractText"),
            (ProfileDocuments, "examples.texts.transforms.profile.ProfileDocuments"),
            (AnalyzeText, "examples.texts.transforms.analyze.AnalyzeText"),
            (CorpusText, "examples.texts.transforms.corpus.CorpusText"),
        )
        files = {}
        for transform_class, source_transform in transforms:
            files.update(
                PySpark.render.project()(
                    PySpark.plan.lower()(compile_transform(transform_class)),
                    source_transform=source_transform,
                    generated_package="examples.structure_generated.texts",
                    source_schema_modules=schema_modules,
                )
            )
        docs = Docs.render.project()(
            StructureConfig.resolve(
                project_root=ROOT,
                source_roots=["examples"],
                generated_dir="examples/structure_generated/texts",
                generated_package="examples.structure_generated.texts",
            ),
            DiscoveredStructureProject(
                transforms=tuple(transform for transform, _ in transforms),
                schema_modules={module: tuple(schemas) for module, schemas in schema_modules.items()},
            ),
        )
        files.update({f"examples/structure_generated/texts/{path}": text for path, text in docs.items()})
        files["examples/structure_generated/texts/traceability/__init__.py"] = (
            "# Generated traceability package marker.\n"
        )
        files["examples/structure_generated/texts/traceability/transforms/__init__.py"] = (
            "# Generated transform traceability package marker.\n"
        )
        return {path: text.rstrip() + "\n" for path, text in files.items()}


def expected_texts_generated() -> dict[str, str]:
    return _expected_generated("texts")


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
        _drop("examples.orders")
        _drop("examples.streams")
        _drop("examples.stocks")
        _drop("examples.texts")
        _drop("examples.structure_generated")


def _drop(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            sys.modules.pop(name, None)
