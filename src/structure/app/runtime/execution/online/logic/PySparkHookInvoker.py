from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.runtime.execution.logic.SparkConnectRuntimeDiagnostics import spark_connect_runtime_error
from structure.app.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe


class HookInputs:

    def __init__(self, **inputs) -> None:
        object.__setattr__(self, "_structure_frozen", False)
        object.__setattr__(self, "_structure_names", tuple(inputs))
        for name, value in inputs.items():
            object.__setattr__(self, name, value)
        object.__setattr__(self, "_structure_frozen", True)

    def __setattr__(self, name, value) -> None:
        if getattr(self, "_structure_frozen", False):
            raise AttributeError("HookInputs is read-only")
        object.__setattr__(self, name, value)


class PySparkHookInvoker:

    def apply(
        self,
        hooks: tuple[PySparkHookRecipe, ...],
        *,
        frames: dict[str, object],
        inputs,
        invocation: Transform,
        session,
    ) -> None:
        for hook in hooks:
            kwargs = {lane: frames[lane] for lane in hook.lanes}
            kwargs.update({"spark": session.spark, "ctx": session.ctx})
            if hook.pass_inputs:
                kwargs["inputs"] = inputs
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
