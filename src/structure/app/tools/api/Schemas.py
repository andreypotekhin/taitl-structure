from structure.app.tools.commands.GenerateStructureSchema import GenerateStructureSchema


class Schemas:

    @staticmethod
    def generate(**kwargs) -> str:
        return GenerateStructureSchema()(**kwargs)
