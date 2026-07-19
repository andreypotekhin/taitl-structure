from __future__ import annotations

import ast
import inspect
from pathlib import Path

from structure.core.dsl.model.expr import expressions
from structure.core.dsl.model.transforms import operations
from structure.platform.pyspark.capabilities.PySparkCapabilityRules import COMMON_CAPABILITIES
from structure.platform.pyspark.commands.ClassifyStreamingCompatibility import ClassifyStreamingCompatibility
from structure.platform.pyspark.commands.RenderPySparkExpression import RenderPySparkExpression
from structure.platform.pyspark.commands.RenderPySparkTransformModule import RenderPySparkTransformModule
from structure.platform.pyspark.execution.online.logic.PySparkExpressionEvaluator import PySparkExpressionEvaluator
from structure.platform.pyspark.logic.mapping.PySparkExpressionMapper import PySparkExpressionMapper


def test_generated_and_online_transform_expression_dispatch_are_identical() -> None:
    renderer_functions = _dispatched_functions(RenderPySparkExpression)
    evaluator_functions = _dispatched_functions(PySparkExpressionEvaluator)

    assert renderer_functions == evaluator_functions


def test_all_v3_dynamic_window_functions_are_dispatched_by_both_backends() -> None:
    functions = _dispatched_functions(RenderPySparkExpression)

    assert _WINDOW_FUNCTIONS <= functions
    assert {("window", _window_capability(function)) for function in _WINDOW_FUNCTIONS} <= COMMON_CAPABILITIES


def test_declared_collection_transformations_have_dispatch_and_capabilities() -> None:
    requirements = _declared_collection_requirements()

    assert {function for _, function in requirements} <= _dispatched_functions(RenderPySparkExpression)
    assert requirements <= COMMON_CAPABILITIES


def test_declared_scalar_helpers_have_generated_and_online_dispatch() -> None:
    helpers = _declared_scalar_helpers()

    assert helpers <= _dispatched_functions(RenderPySparkExpression)
    assert helpers <= _dispatched_functions(PySparkExpressionEvaluator)


def test_transform_expression_consumer_catalog_covers_every_phase() -> None:
    consumers = {
        "mapper": PySparkExpressionMapper,
        "renderer": RenderPySparkExpression,
        "online evaluator": PySparkExpressionEvaluator,
        "generated-module discovery": RenderPySparkTransformModule,
        "stream classifier": ClassifyStreamingCompatibility,
    }

    for name, consumer in consumers.items():
        source = Path(inspect.getfile(consumer)).read_text(encoding="utf-8")
        assert "transform_expression" in source, f"{name} does not handle transform expressions"

    requirements = _declared_collection_requirements()
    assert {function for _, function in requirements} <= _dispatched_functions(RenderPySparkExpression)
    assert requirements <= COMMON_CAPABILITIES


_WINDOW_FUNCTIONS = frozenset(
    {
        "window_row_number",
        "window_rank",
        "window_dense_rank",
        "window_percent_rank",
        "window_cume_dist",
        "window_ntile",
        "window_lag",
        "window_lead",
        "window_first_value",
        "window_last_value",
        "window_nth_value",
        "window_sum",
        "window_avg",
        "window_min",
        "window_max",
        "window_count",
        "window_bool_and",
        "window_bool_or",
        "window_stddev",
        "window_variance",
        "window_collect_list",
        "window_collect_set",
        "window_rolling_sum",
        "window_rolling_avg",
        "window_rolling_min",
        "window_rolling_max",
    }
)


def _dispatched_functions(component: type) -> set[str]:
    source = Path(inspect.getfile(component)).read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare) or not isinstance(node.left, ast.Name) or node.left.id != "function":
            continue
        if len(node.ops) != 1 or len(node.comparators) != 1:
            continue
        comparator = node.comparators[0]
        if (
            isinstance(node.ops[0], ast.Eq)
            and isinstance(comparator, ast.Constant)
            and isinstance(comparator.value, str)
        ):
            functions.add(comparator.value)
        if isinstance(node.ops[0], ast.In) and isinstance(comparator, (ast.Set, ast.Tuple)):
            functions.update(
                item.value for item in comparator.elts if isinstance(item, ast.Constant) and isinstance(item.value, str)
            )
    return functions


def _declared_collection_requirements() -> set[tuple[str, str]]:
    tree = ast.parse(Path(inspect.getfile(operations)).read_text(encoding="utf-8"))
    requirements: set[tuple[str, str]] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id == "_reserved_expression":
            requirement = _reserved_requirement(node)
            if requirement is not None:
                requirements.add(requirement)
        if node.func.id in {"_array_set_operation", "_element_lookup"} and node.args:
            function = _constant_string(node.args[0])
            if function is not None:
                requirements.add(("higher_order", function))
    return requirements


def _declared_scalar_helpers() -> set[str]:
    tree = ast.parse(Path(inspect.getfile(expressions)).read_text(encoding="utf-8"))
    helpers: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id == "_string_call" and node.args:
            function = _constant_string(node.args[0])
            if function is not None:
                helpers.add(function)
        if node.func.id == "Expression":
            data = next((keyword.value for keyword in node.keywords if keyword.arg == "data"), None)
            function = _function_data_value(data)
            if function is not None:
                helpers.add(function)
    return helpers


def _function_data_value(node: ast.expr | None) -> str | None:
    if not isinstance(node, ast.Dict):
        return None
    for key, value in zip(node.keys, node.values, strict=True):
        if _constant_string(key) == "function":
            return _constant_string(value)
    return None


def _reserved_requirement(node: ast.Call) -> tuple[str, str] | None:
    group = next((keyword.value for keyword in node.keywords if keyword.arg == "group"), None)
    name = next((keyword.value for keyword in node.keywords if keyword.arg == "name"), None)
    group_name = _constant_string(group)
    capability_name = _constant_string(name)
    return (group_name, capability_name) if group_name is not None and capability_name is not None else None


def _constant_string(node: ast.expr | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _window_capability(function: str) -> str:
    return (
        f"rolling_{function.removeprefix('window_rolling_')}"
        if function.startswith("window_rolling_")
        else function.removeprefix("window_")
    )
