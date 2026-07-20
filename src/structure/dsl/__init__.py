from importlib import import_module

_EXPORTS = {
    "FieldDeclaration": "structure.core.dsl.model.schemas.FieldDeclaration",
    "FieldDefinition": "structure.core.dsl.model.schemas.FieldDefinition",
    "InputDeclaration": "structure.core.dsl.model.transforms.InputDeclaration",
    "LaneDeclaration": "structure.core.dsl.model.transforms.LaneDeclaration",
    "OutputDeclaration": "structure.core.dsl.model.transforms.OutputDeclaration",
    "Schema": "structure.core.dsl.model.schemas.Schema",
    "SchemaMode": "structure.core.dsl.model.transforms.SchemaMode",
    "StreamingMode": "structure.core.dsl.model.transforms.StreamingMode",
    "Transform": "structure.core.dsl.model.transforms.Transform",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    try:
        module = import_module(_EXPORTS[name])
    except KeyError as error:
        raise AttributeError(name) from error
    value = getattr(module, name)
    globals()[name] = value
    return value
