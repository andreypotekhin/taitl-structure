from structure.core.tools.commands.GenerateStructureSchema import GenerateStructureSchema


class Schemas:

    def generate(self, **kwargs) -> str:
        return GenerateStructureSchema()(**kwargs)
