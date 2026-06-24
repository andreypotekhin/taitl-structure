from structure.app.compiler.ir.model.InputPlan import InputPlan
from structure.app.dsl.model.transforms.Transform import Transform


class CompilerInputCollector:

    def collect(self, transform_class: type[Transform]) -> list[InputPlan]:
        return [
            InputPlan(name=declaration.name, schema=declaration.schema, ordinal=ordinal)
            for ordinal, declaration in enumerate(transform_class._structure_inputs.values())
        ]
