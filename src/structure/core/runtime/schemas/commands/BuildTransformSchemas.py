from typing import cast

from structure.core.runtime.schemas.model.TransformSchemas import TransformSchemas
from structure.platform.api.v1.model import TransformSchemaRequest


class BuildTransformSchemas:
    def __call__(self, payload: object, *, types=None) -> TransformSchemas:
        target = getattr(getattr(payload, "backend", None), "name", None)
        if not isinstance(target, str):
            raise ValueError("PLATFORM-E2708: Schema materialization requires a platform-owned payload.")
        from structure.core.platforms.api.Platform import Platform

        return cast(
            TransformSchemas,
            Platform.registry().select(target).api.schema.build(TransformSchemaRequest(payload=payload, types=types)),
        )
