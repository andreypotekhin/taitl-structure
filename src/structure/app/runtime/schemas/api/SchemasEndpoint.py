from structure.app.runtime.schemas.logic.actions.BuildTransformSchemas import BuildTransformSchemas


class SchemasEndpoint:

    def build(self) -> BuildTransformSchemas:
        return BuildTransformSchemas()
