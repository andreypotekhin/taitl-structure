from structure.app.runtime.schemas.commands.BuildTransformSchemas import BuildTransformSchemas


class Schemas:

    @staticmethod
    def build() -> BuildTransformSchemas:
        return BuildTransformSchemas()
