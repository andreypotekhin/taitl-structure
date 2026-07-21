from typing import cast

from structure.core.runtime.schemas.model.TransformSchemas import TransformSchemas
from structure.plugin.api.v1.model import TransformSchemaRequest


class BuildTransformSchemas:
    def __call__(self, payload: object, *, types=None) -> TransformSchemas:
        target = getattr(getattr(payload, "backend", None), "name", None)
        if not isinstance(target, str):
            raise ValueError("PLUGIN-E2708: Schema materialization requires a plugin-owned payload.")
        from structure.core.plugins.api.Plugin import Plugin

        return cast(
            TransformSchemas,
            Plugin.registry().select(target).api.schema.build(TransformSchemaRequest(payload=payload, types=types)),
        )
