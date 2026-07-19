from structure.core.dsl.model.transforms.Transform import Transform
from structure.platform.api.v1 import InputPlan


class CompilerInputCollector:

    def collect(self, transform_class: type[Transform]) -> list[InputPlan]:
        return [
            InputPlan(
                name=declaration.name,
                schema=declaration.schema,
                ordinal=ordinal,
                streaming=declaration.streaming,
                aliases=declaration.aliases,
            )
            for ordinal, declaration in enumerate(transform_class._structure_inputs.values())
        ]
