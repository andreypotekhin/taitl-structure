import ast
from pathlib import Path
from typing import cast

from structure.core.compiler.compileability.streaming_compatibility.model.StreamingSupport import (
    StreamingSupport as LegacyStreamingSupport,
)
from structure.core.compiler.ir.model.AggregateAssignment import AggregateAssignment as LegacyAggregateAssignment
from structure.core.compiler.ir.model.AggregateKey import AggregateKey as LegacyAggregateKey
from structure.core.compiler.ir.model.AggregatePlan import AggregatePlan as LegacyAggregatePlan
from structure.core.compiler.ir.model.CachePlan import CachePlan as LegacyCachePlan
from structure.core.compiler.ir.model.DuplicateRowsPlan import DuplicateRowsPlan as LegacyDuplicateRowsPlan
from structure.core.compiler.ir.model.JoinMethod import JoinMethod as LegacyJoinMethod
from structure.core.compiler.ir.model.JoinPlan import JoinPlan as LegacyJoinPlan
from structure.core.compiler.ir.model.OperationCapability import OperationCapability as LegacyOperationCapability
from structure.core.compiler.ir.model.OperationCardinality import OperationCardinality as LegacyOperationCardinality
from structure.core.compiler.ir.model.OperationPlan import OperationPlan as LegacyOperationPlan
from structure.core.compiler.ir.model.ProjectAssignment import ProjectAssignment as LegacyProjectAssignment
from structure.core.compiler.ir.model.SelectedRowsPlan import SelectedRowsPlan as LegacySelectedRowsPlan
from structure.core.compiler.ir.model.WatermarkPlan import WatermarkPlan as LegacyWatermarkPlan
from structure.core.dsl.model.expr.Expression import Expression as LegacyExpression
from structure.core.dsl.model.expr.expressions import literal as legacy_literal
from structure.core.dsl.model.schemas.Projection import Projection as LegacyProjection
from structure.core.dsl.model.transforms.AsOf import AsOf as LegacyAsOf
from structure.core.dsl.model.transforms.Join import Join as LegacyJoin
from structure.core.dsl.model.transforms.JoinDedupe import JoinDedupe as LegacyJoinDedupe
from structure.core.dsl.model.transforms.JoinHint import JoinHint as LegacyJoinHint
from structure.core.dsl.model.transforms.JoinStrategy import JoinStrategy as LegacyJoinStrategy
from structure.core.dsl.model.transforms.operations import count as legacy_count
from structure.core.dsl.model.transforms.OverlapPolicy import OverlapPolicy as LegacyOverlapPolicy
from structure.core.dsl.model.transforms.StreamingOutputMode import StreamingOutputMode as LegacyStreamingOutputMode
from structure.core.dsl.model.transforms.TiePolicy import TiePolicy as LegacyTiePolicy
from structure.core.dsl.model.types.ArrayType import ArrayType as LegacyArrayType
from structure.core.dsl.model.types.DecimalType import DecimalType as LegacyDecimalType
from structure.core.dsl.model.types.StructType import StructType as LegacyStructType
from structure.plugin.pyspark import (
    AsOf,
    Join,
    JoinDedupe,
    JoinHint,
    JoinMethod,
    JoinPlan,
    JoinStrategy,
    OverlapPolicy,
    PySpark,
    TiePolicy,
)
from structure.plugin.pyspark.compiler.api.Compiler import Compiler, LowerPySparkPlan
from structure.plugin.pyspark.dsl.aggregation import AggregateAssignment, AggregateKey, AggregatePlan, ProjectAssignment
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.operations import (
    CachePlan,
    DuplicateRowsPlan,
    OperationCapability,
    OperationCardinality,
    OperationPlan,
    SelectedRowsPlan,
    StreamingOutputMode,
    StreamingSupport,
    WatermarkPlan,
)
from structure.plugin.pyspark.dsl.operations_api import count
from structure.plugin.pyspark.dsl.Projection import Projection
from structure.plugin.pyspark.dsl.types import ArrayType, DecimalType, StructType
from structure.plugin.pyspark.files.api.Files import CompareGeneratedFiles
from structure.plugin.pyspark.render.api.Render import Render, RenderPySparkProject
from structure.plugin.pyspark.schema.api.Schema import MaterializePySparkSchema, Schema
from structure.plugin.pyspark.symbolic_execution.api.SymbolicExecution import SymbolicExecution
from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import (
    PySparkSymbolicContext,
    current_pyspark_context,
)


def test_pyspark_endpoint_groups_commands_and_creates_fresh_actions() -> None:
    assert isinstance(PySpark.compiler, Compiler)
    assert isinstance(PySpark.schema, Schema)
    assert isinstance(PySpark.render, Render)
    assert isinstance(PySpark.symbolic_execution, SymbolicExecution)
    assert isinstance(PySpark.compiler.lower(), LowerPySparkPlan)
    assert isinstance(PySpark.schema.materialize(), MaterializePySparkSchema)
    assert isinstance(PySpark.render.project(), RenderPySparkProject)
    assert isinstance(PySpark.files.compare(), CompareGeneratedFiles)
    assert PySpark.compiler.lower() is not PySpark.compiler.lower()


def test_pyspark_symbolic_execution_scopes_its_authoring_context() -> None:
    symbolic = cast(PySparkSymbolicContext, PySpark.symbolic_execution.open()(step="publish", capture_special_exprs=True))

    assert current_pyspark_context() is None
    with symbolic:
        assert current_pyspark_context() is symbolic
        assert symbolic.capture_special_exprs
        symbolic.register_current_scope("rows")
        assert symbolic.register_relation_scope("customers", object()) is symbolic.relation_scopes["customers"]
    assert current_pyspark_context() is None


def test_pyspark_join_policies_are_owned_by_the_plugin_dsl() -> None:
    legacy_types = (
        LegacyAsOf,
        LegacyJoin,
        LegacyJoinDedupe,
        LegacyJoinHint,
        LegacyJoinMethod,
        LegacyJoinPlan,
        LegacyJoinStrategy,
        LegacyOverlapPolicy,
        LegacyTiePolicy,
    )
    plugin_types = (AsOf, Join, JoinDedupe, JoinHint, JoinMethod, JoinPlan, JoinStrategy, OverlapPolicy, TiePolicy)

    assert legacy_types == plugin_types
    assert all(type_.__module__.startswith("structure.plugin.pyspark.dsl.joins.") for type_ in plugin_types)


def test_pyspark_operation_records_are_owned_by_the_plugin_dsl() -> None:
    legacy_types = (
        LegacyCachePlan,
        LegacyDuplicateRowsPlan,
        LegacyOperationCapability,
        LegacyOperationCardinality,
        LegacyOperationPlan,
        LegacySelectedRowsPlan,
        LegacyStreamingOutputMode,
        LegacyStreamingSupport,
        LegacyWatermarkPlan,
    )
    plugin_types = (
        CachePlan,
        DuplicateRowsPlan,
        OperationCapability,
        OperationCardinality,
        OperationPlan,
        SelectedRowsPlan,
        StreamingOutputMode,
        StreamingSupport,
        WatermarkPlan,
    )

    assert legacy_types == plugin_types
    assert all(type_.__module__.startswith("structure.plugin.pyspark.dsl.operations.") for type_ in plugin_types)


def test_pyspark_projection_and_aggregation_records_are_owned_by_the_plugin_dsl() -> None:
    legacy_types = (LegacyAggregateAssignment, LegacyAggregateKey, LegacyAggregatePlan, LegacyProjectAssignment)
    plugin_types = (AggregateAssignment, AggregateKey, AggregatePlan, ProjectAssignment)

    assert legacy_types == plugin_types
    assert all(type_.__module__.startswith("structure.plugin.pyspark.dsl.aggregation.") for type_ in plugin_types)


def test_pyspark_concrete_types_are_owned_by_the_plugin_dsl() -> None:
    assert (LegacyArrayType, LegacyDecimalType, LegacyStructType) == (ArrayType, DecimalType, StructType)
    assert all(
        type_.__module__.startswith("structure.plugin.pyspark.dsl.types.")
        for type_ in (ArrayType, DecimalType, StructType)
    )


def test_pyspark_expression_graph_is_owned_by_the_plugin_dsl() -> None:
    assert LegacyExpression is Expression
    assert Expression.__module__ == "structure.plugin.pyspark.dsl.Expression"
    assert legacy_literal("value").kind == literal("value").kind == "literal"
    assert literal.__module__ == "structure.plugin.pyspark.dsl.expressions"


def test_pyspark_operation_helpers_are_owned_by_the_plugin_dsl() -> None:
    assert legacy_count().kind == count().kind == "aggregate"
    assert count.__module__ == "structure.plugin.pyspark.dsl.operations_api"


def test_pyspark_projection_body_is_owned_by_the_plugin_dsl() -> None:
    assert LegacyProjection is Projection
    assert Projection.__module__ == "structure.plugin.pyspark.dsl.Projection"


def test_pyspark_apps_do_not_import_another_apps_private_commands_or_logic() -> None:
    root = Path("src/structure/plugin/pyspark")
    violations: list[str] = []
    for source in root.rglob("*.py"):
        relative = source.relative_to(root)
        owner = relative.parts[0]
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.module is None:
                continue
            parts = node.module.split(".")
            prefix = ("structure", "plugin", "pyspark")
            if tuple(parts[:3]) != prefix or len(parts) < 5:
                continue
            target, boundary = parts[3:5]
            if owner != target and boundary in {"commands", "logic"}:
                violations.append(f"{relative}: {node.module}")
    assert not violations, "PySpark apps must invoke peers through their API endpoints:\n" + "\n".join(violations)


def test_pyspark_app_readmes_follow_the_core_app_format() -> None:
    for readme in Path("src/structure/plugin/pyspark").rglob("Readme.md"):
        content = readme.read_text(encoding="utf-8")
        for heading in ("## Purpose", "## Dependency Exchanges", "## Inner Workings"):
            assert heading in content, f"{readme} is missing {heading}"


def test_pyspark_dsl_does_not_import_core_concrete_target_models() -> None:
    root = Path("src/structure/plugin/pyspark")
    forbidden = (
        "structure.core.dsl.model.expr",
        "structure.core.dsl.model.schemas.Projection",
        "structure.core.dsl.model.types",
    )
    violations = []
    for source in root.rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith(forbidden):
                violations.append(f"{source.relative_to(root)}: {node.module}")
    assert not violations, "PySpark target models must be imported from pyspark.dsl:\n" + "\n".join(violations)


def test_pyspark_plugin_uses_only_public_structure_contracts() -> None:
    root = Path("src/structure/plugin/pyspark")
    violations = []
    for source in root.rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("structure.core"):
                violations.append(f"{source.relative_to(root)}: {node.module}")
    assert not violations, "PySpark must use structure.dsl or structure.plugin.api.v1, never Core internals:\n" + "\n".join(violations)


def test_plugin_api_uses_only_public_structure_contracts() -> None:
    root = Path("src/structure/plugin")
    violations = []
    for source in root.rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("structure.core"):
                violations.append(f"{source.relative_to(root)}: {node.module}")
    assert not violations, "Plugin contracts and plugins must never import Core internals:\n" + "\n".join(violations)
