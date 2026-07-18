from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.runtime.execution.logic.SparkConnectRuntimeDiagnostics import spark_connect_runtime_error
from structure.core.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe


class PySparkHookInvoker:

    def apply(
        self,
        hooks: tuple[PySparkHookRecipe, ...],
        *,
        frames: dict[str, object],
        invocation: Transform,
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

    def _call(self, hook: PySparkHookRecipe, invocation: Transform, kwargs: dict[str, object]):
        origin = hook.origin
        if origin is None or origin.owner is None:
            return getattr(invocation, hook.name)(**kwargs)
        function = origin.owner.__dict__.get(origin.member_name)
        if function is None:
            return getattr(invocation, hook.name)(**kwargs)
        return function(invocation, **kwargs)
