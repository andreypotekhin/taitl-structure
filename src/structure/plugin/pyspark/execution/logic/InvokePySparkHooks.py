from typing import Any, cast

from structure.dsl import Transform
from structure.plugin.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.plugin.pyspark.execution.logic.SparkConnectRuntimeDiagnostics import spark_connect_runtime_error


class InvokePySparkHooks:

    def apply(
        self,
        hooks: tuple[PySparkHookRecipe, ...],
        *,
        frames: dict[str, object],
        invocation: Any,
        session,
    ) -> None:
        for hook in hooks:
            kwargs = {lane: frames[source] for lane, source in zip(hook.lanes, hook.sources, strict=True)}
            kwargs.update({"spark": session.spark, "ctx": session.ctx})
            try:
                result = self._call(hook, invocation, kwargs)
            except Exception as error:
                boundary = spark_connect_runtime_error(
                    invocation,
                    session=session,
                    error=error,
                    surface=f"hook {hook.name}",
                )
                if boundary is not None:
                    raise boundary from error
                raise
            if len(hook.outputs) == 1:
                frames[hook.outputs[0]] = result
                continue
            if not isinstance(result, tuple) or len(result) != len(hook.outputs):
                raise TypeError(
                    f"Hook {hook.name} must return {len(hook.outputs)} DataFrames for outputs: "
                    f"{', '.join(hook.outputs)}"
                )
            for name, frame in zip(hook.outputs, result, strict=True):
                frames[name] = frame

    def _call(self, hook: PySparkHookRecipe, invocation: Any, kwargs: dict[str, object]):
        origin = hook.origin
        if origin is None or origin.owner is None:
            return getattr(invocation, hook.name)(**kwargs)
        owner = cast(type[Transform], origin.owner)
        function = owner.__dict__.get(origin.member_name)
        if function is None:
            return getattr(invocation, hook.name)(**kwargs)
        return function(self._owner_invocation(hook, invocation, owner), **kwargs)

    def _owner_invocation(self, hook: PySparkHookRecipe, invocation: Any, owner: type[Transform]) -> Transform:
        if isinstance(invocation, owner):
            return invocation
        target_stage = self._hook_stage(hook)
        pipeline_stages = getattr(invocation, "stages", ())
        for label, stage in zip(self._pipeline_labels(pipeline_stages), pipeline_stages, strict=True):
            candidate = getattr(stage, "invocation", None)
            if label == target_stage and isinstance(candidate, owner):
                return candidate
        for stage in pipeline_stages:
            candidate = getattr(stage, "invocation", None)
            if isinstance(candidate, owner):
                return candidate
        graph_stages = getattr(type(invocation), "_structure_stages", {})
        stage = graph_stages.get(target_stage)
        candidate = getattr(stage, "invocation", None)
        if isinstance(candidate, owner):
            return candidate
        for stage in graph_stages.values():
            candidate = getattr(stage, "invocation", None)
            if isinstance(candidate, owner):
                return candidate
        return owner()

    def _hook_stage(self, hook: PySparkHookRecipe) -> str:
        return hook.target.split(".", 1)[0] if "." in hook.target else ""

    def _pipeline_labels(self, stages) -> tuple[str, ...]:
        counts: dict[str, int] = {}
        labels: list[str] = []
        for stage in stages:
            base = self._snake(type(stage.invocation).__name__)
            counts[base] = counts.get(base, 0) + 1
            labels.append(base if counts[base] == 1 else f"{base}_{counts[base]}")
        return tuple(labels)

    def _snake(self, name: str) -> str:
        result: list[str] = []
        for index, character in enumerate(name):
            if character.isupper() and index > 0:
                result.append("_")
            result.append(character.lower())
        return "".join(result)
