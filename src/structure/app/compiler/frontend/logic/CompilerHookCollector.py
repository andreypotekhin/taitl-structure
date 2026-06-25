from structure.app.compiler.ir.model.HookPlan import HookPlan
from structure.app.dsl.model.transforms.Transform import Transform


class CompilerHookCollector:

    def collect(self, transform_class: type[Transform]) -> dict[tuple[str, str], tuple[HookPlan, ...]]:
        grouped: dict[tuple[str, str], list[HookPlan]] = {}
        for name, member in transform_class.__dict__.items():
            metadata = getattr(member, "_structure_hook", None)
            if metadata is None:
                continue

            key = (metadata["phase"], metadata["target"])
            grouped.setdefault(key, []).append(
                HookPlan(
                    name=name,
                    phase=metadata["phase"],
                    target=metadata["target"],
                    lanes=metadata["lanes"],
                    outputs=metadata["outputs"],
                    pass_inputs=metadata["pass_inputs"],
                    schema_mode=metadata["schema_mode"],
                    project_output=metadata["project_output"],
                    streaming_safe=metadata["streaming_safe"],
                )
            )
        return {key: tuple(value) for key, value in grouped.items()}
