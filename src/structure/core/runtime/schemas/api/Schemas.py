from structure.core.runtime.schemas.commands.BuildTransformSchemas import BuildTransformSchemas


class Schemas:

    @staticmethod
    def build() -> BuildTransformSchemas:
        return BuildTransformSchemas()
