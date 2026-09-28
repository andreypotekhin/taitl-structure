from structure.plugin.api.v1 import OptimizationGraph, OptimizationStep
from structure.plugin.pyspark.compiler.logic.RemovalSafety import RemovalSafety
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class DescribePySparkOptimization:
    """Resolve reads against the producer visible at each point in source order."""

    def __call__(self, plan: PySparkExecutionPlan) -> OptimizationGraph:
        producers: dict[str, str | None] = {
            name: None for item in plan.inputs for name in (item.name, f"input:{item.name}")
        }
        safety = RemovalSafety()
        nodes: list[OptimizationStep] = []
        positions: dict[str, int] = {}
        for step in plan.steps:
            reason = safety.reason(step)
            reads = [step.source, *step.input_sources, *(join.source for join in step.joins)]
            declared = set(reads)
            hooks = (*step.before_hooks, *step.after_hooks, *(h for result in step.results for h in result.after_hooks))
            reads.extend(source for hook in hooks for source in hook.sources)
            for operation in step.operations:
                if operation.join is not None:
                    reads.append(operation.join.source)
                if operation.relation_set is not None:
                    reads.append(operation.relation_set.source)
                if operation.relation_alias is not None:
                    reads.append(operation.relation_alias.source)
                if operation.relation_assertion is not None and operation.relation_assertion.reference_source:
                    reads.append(operation.relation_assertion.reference_source)
                if operation.relation_hierarchy_fallback is not None:
                    reads.append(operation.relation_hierarchy_fallback.parent_source)
            local = {result.frame for result in step.results} | {source for hook in hooks for source in hook.outputs}
            dependencies: set[str] = set()
            for source in reads:
                if source in producers:
                    producer = producers[source]
                    if producer is not None:
                        dependencies.add(producer)
                elif source not in local:
                    if source in declared:
                        raise self._error(f"step {step.name} reads unknown frame {source}")
                    # Some composed operation payloads still use local rather than rewritten frame names.
                    # Do not reinterpret them or change runtime semantics: retain all possible prerequisites.
                    dependencies.update(node.name for node in nodes)
                    reason = reason or f"unknown producer for operation frame {source}"
            if hooks or (reason is not None and reason.startswith("unknown removal safety for operation")):
                # Opaque operations can read or replace frames beyond their declared selectors.
                dependencies.update(node.name for node in nodes)
            nodes.append(OptimizationStep(
                name=step.name,
                dependencies=tuple(sorted(dependencies, key=positions.__getitem__)),
                removable=reason is None,
                reason=reason or "known pure computation",
            ))
            positions[step.name] = len(nodes) - 1
            for frame in local:
                producers[frame] = step.name

        def roots(outputs):
            result: set[str] = set()
            for output in outputs:
                if output.operations:
                    # Output operations can contain further relation reads; retain their prerequisites conservatively.
                    result.update(positions)
                for source in (output.source, *(join.source for join in output.joins)):
                    if source not in producers:
                        raise self._error(f"output {output.name} reads unknown frame {source}")
                    producer = producers[source]
                    if producer is not None:
                        result.add(producer)
            return tuple(node.name for node in nodes if node.name in result)

        return OptimizationGraph(
            steps=tuple(nodes),
            output_roots=roots(plan.outputs),
            stage_roots=roots(item.output for item in plan.stage_outputs) if plan.allow_stage_outputs else (),
            allow_stage_outputs=plan.allow_stage_outputs,
        )

    @staticmethod
    def _error(problem):
        return ValueError(
            f"PLUGIN-E2708: Invalid PySpark optimization dependencies: {problem}. "
            "Check the plugin's ordered producer mapping; see docs/dev/PluginAuthoring.md#unused-step-optimization."
        )
