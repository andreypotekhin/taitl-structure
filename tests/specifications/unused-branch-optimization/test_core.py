import pickle
from dataclasses import replace
from types import SimpleNamespace

import pytest

from structure import Transform
from structure.core.compiler.frontend.commands.CompilePluginTransform import CompilePluginTransform
from structure.core.compiler.frontend.commands.PruneUnusedSteps import PruneUnusedSteps
from structure.plugin.api import PluginDescriptor
from structure.plugin.api.conformance import PluginConformance
from structure.plugin.api.v1 import (
    CompilationPurpose,
    CompileRequest,
    OptimizationGraph,
    OptimizationStep,
    PluginAPI,
    PluginCompilation,
    TransformPlan,
)


class NeutralOptimizer:
    def __init__(self, graph):
        self.graph = graph

    def describe(self, request):
        retained = request.compilation.executable_steps
        if retained == tuple(node.name for node in self.graph.steps):
            return self.graph
        return replace(
            self.graph,
            steps=tuple(node for node in self.graph.steps if node.name in retained),
            stage_roots=tuple(name for name in self.graph.stage_roots if name in retained),
        )

    def apply(self, request, selection):
        retained = selection.retained_steps
        return replace(
            request.compilation,
            lowered=tuple(item for item in request.compilation.lowered if item[0] in retained),
            executable_steps=retained,
        )


def optimize(graph, *, inventory=None, optimizer=None):
    inventory = tuple(node.name for node in graph.steps) if inventory is None else inventory
    return PruneUnusedSteps()(
        PluginCompilation(tuple((name, index) for index, name in enumerate(inventory)), "fixture", executable_steps=inventory),
        request=CompileRequest(Transform, "neutral", {}),
        optimizer=optimizer or NeutralOptimizer(graph),
    )


def graph():
    return OptimizationGraph(
        steps=(
            OptimizationStep("shared", removable=True),
            OptimizationStep("unused", ("shared",), removable=True),
            OptimizationStep("result", ("shared",), removable=True),
            OptimizationStep("check", ("shared",), reason="it contains a required assertion"),
        ),
        output_roots=("result",),
        stage_roots=("unused",),
        allow_stage_outputs=False,
    )


def test_neutral_graph_retains_results_safety_and_dependencies():
    result = optimize(graph())
    assert result.executable_steps == ("shared", "result", "check")
    assert result.lowered == (("shared", 0), ("result", 2), ("check", 3))
    assert result.optimization.removed_steps == ("unused",)
    assert "4 original, 3 retained" in result.optimization.explain()
    assert pickle.loads(pickle.dumps(result)) == result
    assert PruneUnusedSteps()(
        result, request=CompileRequest(Transform, "neutral", {}), optimizer=NeutralOptimizer(graph())
    ) is result
    assert result.optimization is not None
    with pytest.raises(ValueError, match="existing optimization report"):
        PruneUnusedSteps()(
            replace(result, optimization=replace(result.optimization, decisions=())),  # type: ignore[arg-type]
            request=CompileRequest(Transform, "neutral", {}), optimizer=NeutralOptimizer(graph()),
        )


def test_optimizer_is_an_additive_conforming_plugin_facet():
    from unittest.mock import Mock

    api = PluginAPI(Mock(), Mock(), Mock(), Mock(), optimizer=NeutralOptimizer(graph()))
    plugin = SimpleNamespace(
        descriptor=PluginDescriptor("neutral", "Neutral", "neutral-fixture", "1.0", 1, 1),
        api=lambda version: api,
    )
    assert PluginConformance.negotiate(plugin, entry_name="neutral", distribution="neutral-fixture").api is api
    with pytest.raises(ValueError, match="optimizer.apply"):
        plugin.api = lambda version: replace(api, optimizer=SimpleNamespace(describe=lambda request: graph()))
        PluginConformance.negotiate(plugin, entry_name="neutral", distribution="neutral-fixture")


def test_stage_visibility_and_missing_safety_information_are_conservative():
    assert optimize(replace(graph(), allow_stage_outputs=True)).optimization.removed_steps == ()
    uncertain = replace(graph(), steps=tuple(replace(step, removable=False) for step in graph().steps))
    assert optimize(uncertain).optimization.removed_steps == ()


@pytest.mark.parametrize("invalid", [
    OptimizationGraph((OptimizationStep("same"), OptimizationStep("same"))),
    OptimizationGraph((OptimizationStep(""),)),
    OptimizationGraph((OptimizationStep("x", ("missing",)),)),
    OptimizationGraph((OptimizationStep("x", ("x",)),)),
    OptimizationGraph((OptimizationStep("x", ("y",)), OptimizationStep("y", ("x",)))),
    OptimizationGraph((OptimizationStep("x"),), output_roots=("missing",)),
    OptimizationGraph((OptimizationStep("x"),), stage_roots=("missing",), allow_stage_outputs=False),
    OptimizationGraph((OptimizationStep("x", removable="yes"),)),  # type: ignore[arg-type]
])
def test_invalid_plugin_graph_fails_clearly(invalid):
    with pytest.raises(ValueError, match="PLUGIN-E2708.*optimization contract"):
        optimize(invalid)


def test_inventory_and_applied_selection_must_be_complete():
    class Incomplete(NeutralOptimizer):
        def describe(self, request):
            return self.graph

    with pytest.raises(ValueError, match="complete executable_steps"):
        optimize(graph(), inventory=("shared",), optimizer=Incomplete(graph()))

    class Broken(NeutralOptimizer):
        def apply(self, request, selection):
            return request.compilation

    with pytest.raises(ValueError, match="exactly the selected"):
        optimize(graph(), optimizer=Broken(graph()))


@pytest.mark.parametrize("enabled, opted_in, expected", [(True, True, 3), (False, True, 4), (True, False, 4)])
def test_core_orchestrates_opt_in_plugins_and_leaves_legacy_plugins_unchanged(enabled, opted_in, expected):
    inventory = tuple(step.name for step in graph().steps)
    compilation = PluginCompilation(tuple((name, 0) for name in inventory), "neutral", executable_steps=inventory)
    api = SimpleNamespace(
        compiler=SimpleNamespace(compile=lambda request: compilation),
        analysis=None,
        optimizer=NeutralOptimizer(graph()) if opted_in else None,
    )
    plan = TransformPlan("Neutral", (), (), ())
    result = CompilePluginTransform()._compile(
        Transform, plan, target="neutral", configuration={"prune_unused_steps": enabled},
        plugin_options={}, registry=None, plugin=SimpleNamespace(api=api),
    )
    assert isinstance(result.lowered, tuple)
    assert len(result.lowered) == expected
    assert result.analysis is plan


def test_documentation_compilation_does_not_ask_for_optimization():
    class NoOptimization:
        def describe(self, request):
            raise AssertionError("documentation must not request executable facts")

    api = SimpleNamespace(
        compiler=SimpleNamespace(compile=lambda request: PluginCompilation(None, "docs")),
        analysis=None, optimizer=NoOptimization(),
    )
    result = CompilePluginTransform()._compile(
        Transform, TransformPlan("Docs", (), (), ()), target="neutral", configuration={}, plugin_options={},
        registry=None, plugin=SimpleNamespace(api=api), purpose=CompilationPurpose.DOCUMENTATION,
    )
    assert result.lowered is None
