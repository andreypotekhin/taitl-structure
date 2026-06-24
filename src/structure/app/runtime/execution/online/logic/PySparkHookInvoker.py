from structure.app.dsl.model.transforms.Transform import Transform
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
        current,
        inputs,
        invocation: Transform,
        session,
    ):
        df = current
        for hook in hooks:
            kwargs = {"df": df, "spark": session.spark, "ctx": session.ctx}
            if hook.pass_inputs:
                kwargs["inputs"] = inputs
            df = getattr(invocation, hook.name)(**kwargs)
        return df
