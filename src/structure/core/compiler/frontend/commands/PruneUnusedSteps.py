from dataclasses import replace

from structure.plugin.api.v1 import (
    CompileRequest,
    OptimizationAPI,
    OptimizationDecision,
    OptimizationGraph,
    OptimizationReport,
    OptimizationRequest,
    OptimizationStep,
    PluginCompilation,
)


class PruneUnusedSteps:
    """Select whole executable steps without interpreting a plugin's payload."""

    def __call__(
        self, compilation: PluginCompilation, *, request: CompileRequest, optimizer: OptimizationAPI
    ) -> PluginCompilation:
        optimization_request = OptimizationRequest(request, compilation)
        graph = optimizer.describe(optimization_request)
        self._validate(graph, compilation.executable_steps)
        if compilation.optimization is not None:
            if (
                not isinstance(compilation.optimization, OptimizationReport)
                or compilation.optimization.retained_steps != compilation.executable_steps
            ):
                raise self._error("the existing optimization report must match the executable steps")
            return compilation
        selection = self.select(graph)
        result = optimizer.apply(optimization_request, selection)
        if not isinstance(result, PluginCompilation) or result.executable_steps != selection.retained_steps:
            raise self._error("apply() must return exactly the selected executable steps in their original order")
        self._validate(optimizer.describe(OptimizationRequest(request, result)), result.executable_steps)
        return replace(result, optimization=selection)

    def select(self, graph: OptimizationGraph) -> OptimizationReport:
        nodes = {step.name: step for step in graph.steps}
        reasons = {step.name: step.reason for step in graph.steps if not step.removable}
        for name in graph.output_roots:
            reasons.setdefault(name, "a returned output uses it")
        if graph.allow_stage_outputs:
            for name in graph.stage_roots:
                reasons.setdefault(name, "intermediate stage outputs are enabled")
        pending = list(reasons)
        while pending:
            name = pending.pop()
            for dependency in nodes[name].dependencies:
                if dependency not in reasons:
                    reasons[dependency] = f"required step {name} uses it"
                    pending.append(dependency)
        return OptimizationReport(tuple(
            OptimizationDecision(
                step=step.name,
                retained=step.name in reasons,
                reason=reasons.get(step.name, "no returned output or required operation uses it"),
            )
            for step in graph.steps
        ))

    def _validate(self, graph: OptimizationGraph, inventory: tuple[str, ...] | None) -> None:
        if not isinstance(graph, OptimizationGraph) or not isinstance(graph.steps, tuple):
            raise self._error("describe() must return an OptimizationGraph with ordered steps")
        if any(not isinstance(step, OptimizationStep) for step in graph.steps):
            raise self._error("every graph node must be an OptimizationStep")
        names = tuple(step.name for step in graph.steps)
        if any(not isinstance(name, str) or not name for name in names) or len(set(names)) != len(names):
            raise self._error("step identities must be unique nonempty strings")
        if inventory is None or names != inventory:
            raise self._error("graph steps must match the compilation's complete executable_steps inventory")
        if not isinstance(graph.allow_stage_outputs, bool):
            raise self._error("allow_stage_outputs must be Boolean")
        for roots in (graph.output_roots, graph.stage_roots):
            if not isinstance(roots, tuple) or any(not isinstance(name, str) or name not in names for name in roots):
                raise self._error("output roots must name known executable steps")
        earlier: set[str] = set()
        for step in graph.steps:
            if not isinstance(step.removable, bool) or not isinstance(step.reason, str) or not step.reason:
                raise self._error("removal safety must be Boolean and include a nonempty reason")
            if not isinstance(step.dependencies, tuple) or any(
                not isinstance(name, str) or name not in earlier for name in step.dependencies
            ):
                raise self._error(f"dependencies of {step.name} must name earlier steps (no cycles or dangling references)")
            earlier.add(step.name)

    @staticmethod
    def _error(problem: str) -> ValueError:
        return ValueError(
            f"PLUGIN-E2708: Invalid optimization contract: {problem}. "
            "Fix the plugin's optimization facts; see docs/dev/PluginAuthoring.md#unused-step-optimization."
        )
