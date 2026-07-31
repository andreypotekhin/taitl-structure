from structure.core.dsl.model.transforms.Transform import Transform
from structure.plugin.api.v1.model import InputPlan


class CompilerInputCollector:

    def collect(self, transform_class: type[Transform]) -> list[InputPlan]:
        return [
            InputPlan(
                name=declaration.name,
                schema=declaration.schema,
                ordinal=ordinal,
                streaming=declaration.streaming,
                aliases=declaration.aliases,
                streaming_declared=declaration.streaming_declared,
            )
            for ordinal, declaration in enumerate(transform_class._structure_inputs.values())
        ]
