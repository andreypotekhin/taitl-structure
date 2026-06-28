from __future__ import annotations

import keyword
import re

from structure.app.tools.logic.model.GeneratedSchemaClass import GeneratedSchemaClass
from structure.app.tools.logic.model.GeneratedSchemaField import GeneratedSchemaField
from structure.app.tools.logic.model.GeneratedSchemaSource import GeneratedSchemaSource
from structure.app.tools.logic.rules.ValidateSchemaToolRequest import ValidateSchemaToolRequest
from structure.app.tools.model import StructureToolError


class MapPySparkSchemaToStructureSource:

    def __init__(self) -> None:
        self._classes: list[GeneratedSchemaClass] = []
        self._used: set[str] = set()
        self._validate = ValidateSchemaToolRequest()

    def __call__(self, schema, *, to: str) -> GeneratedSchemaSource:
        self._classes = []
        self._used = set()
        self._struct(schema, class_name=to, root=to, path=())
        return GeneratedSchemaSource(tuple(self._classes))

    def _struct(self, schema, *, class_name: str, root: str, path: tuple[str, ...]) -> str:
        self._require_struct(schema, path=path)
        name = self._unique(class_name)
        used_fields: set[str] = set()
        fields = tuple(self._field(field, root=root, path=path, used=used_fields) for field in self._fields(schema))
        self._classes.append(GeneratedSchemaClass(name=name, fields=fields))
        return name

    def _field(self, field, *, root: str, path: tuple[str, ...], used: set[str]) -> GeneratedSchemaField:
        column = str(getattr(field, "name", None))
        self._validate.field(column, path=".".join((*path, column)))
        return GeneratedSchemaField(
            name=self._field_name(column, used=used),
            type=self._type(getattr(field, "dataType", None), root=root, path=(*path, column)),
            nullable=bool(getattr(field, "nullable", True)),
            alias=None if self._identifier(column) else column,
        )

    def _type(self, type, *, root: str, path: tuple[str, ...]) -> str:
        name = type.__class__.__name__
        scalar = {
            "StringType": "String()",
            "IntegerType": "Integer()",
            "LongType": "Long()",
            "FloatType": "Float()",
            "DoubleType": "Double()",
            "BooleanType": "Boolean()",
            "DateType": "Date()",
            "TimestampType": "Timestamp()",
        }.get(name)
        if scalar:
            return scalar
        if name == "DecimalType":
            return f"Decimal({type.precision}, {type.scale})"
        if name == "ArrayType":
            contains_null = "True" if bool(type.containsNull) else "False"
            element = self._type(type.elementType, root=root, path=(*path, "item"))
            return f"Array({element}, contains_null={contains_null})"
        if name == "MapType":
            value_contains_null = "True" if bool(type.valueContainsNull) else "False"
            key = self._type(type.keyType, root=root, path=(*path, "key"))
            value = self._type(type.valueType, root=root, path=(*path, "value"))
            return f"Map({key}, {value}, value_contains_null={value_contains_null})"
        if name == "StructType":
            nested = self._struct(type, class_name=self._nested_name(root, path), root=root, path=path)
            return f"Struct({nested})"
        raise StructureToolError(f"Unsupported Spark type at {'.'.join(path)}: {name}.")

    def _fields(self, schema) -> tuple:
        return tuple(getattr(schema, "fields", ()))

    def _require_struct(self, schema, *, path: tuple[str, ...]) -> None:
        if schema.__class__.__name__ != "StructType" or not hasattr(schema, "fields"):
            location = ".".join(path) if path else "schema"
            raise StructureToolError(f"{location} must be a PySpark StructType.")

    def _nested_name(self, owner: str, path: tuple[str, ...]) -> str:
        return owner + "".join(self._pascal(part) for part in path)

    def _pascal(self, value: str) -> str:
        words = re.split(r"[^0-9A-Za-z]+|_", value)
        return "".join(word[:1].upper() + word[1:] for word in words if word)

    def _field_name(self, column: str, *, used: set[str]) -> str:
        words = [word.lower() for word in re.split(r"[^0-9A-Za-z]+|_", column) if word]
        base = "_".join(words) or "field"
        if base[:1].isdigit():
            base = f"field_{base}"
        if keyword.iskeyword(base):
            base = f"{base}_"
        candidate = base
        index = 2
        while candidate in used:
            candidate = f"{base}_{index}"
            index += 1
        used.add(candidate)
        return candidate

    def _identifier(self, value: str) -> bool:
        return value.isidentifier() and not keyword.iskeyword(value)

    def _unique(self, name: str) -> str:
        candidate = name
        index = 2
        while candidate in self._used or keyword.iskeyword(candidate):
            candidate = f"{name}{index}"
            index += 1
        self._used.add(candidate)
        return candidate
