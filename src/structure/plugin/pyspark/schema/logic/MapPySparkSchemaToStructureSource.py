from __future__ import annotations

import keyword
import re

from structure.plugin.api.v1.model import (
    GeneratedSchemaClass,
    GeneratedSchemaField,
    GeneratedSchemaSource,
    StructureToolError,
    ValidateSchemaToolRequest,
)
from structure.plugin.pyspark.schema.SketchMetadata import DESCRIPTOR_VERSION, METADATA_KEY


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
        metadata = getattr(field, "metadata", {}) or {}
        descriptor = metadata.get(METADATA_KEY)
        return GeneratedSchemaField(
            name=self._field_name(column, used=used),
            type=self._type(getattr(field, "dataType", None), root=root, path=(*path, column), descriptor=descriptor),
            nullable=bool(getattr(field, "nullable", True)),
            alias=None if self._identifier(column) else column,
        )

    def _type(self, type, *, root: str, path: tuple[str, ...], descriptor=None) -> str:
        name = type.__class__.__name__
        if descriptor is not None:
            return self._tagged_type(type, descriptor, root=root, path=path)
        scalar = {
            "StringType": "string()",
            "BinaryType": "binary()",
            "IntegerType": "integer()",
            "LongType": "long()",
            "FloatType": "float()",
            "DoubleType": "double()",
            "BooleanType": "boolean()",
            "DateType": "date()",
            "TimestampType": "timestamp()",
            "TimestampNTZType": "timestamp_ntz()",
            "VariantType": "variant()",
        }.get(name)
        if scalar:
            return scalar
        if name == "DecimalType":
            return f"decimal({type.precision}, {type.scale})"
        if name == "TimeType":
            return f"time({type.precision})"
        if name == "GeometryType":
            if self._is_sedona(type):
                return "sedona.geometry(srid=None)"
            return f"geometry(srid={self._native_srid(type)!r})"
        if name == "GeographyType":
            return f"geography(srid={self._native_srid(type)!r})"
        if name == "ArrayType":
            contains_null = "True" if bool(type.containsNull) else "False"
            element = self._type(type.elementType, root=root, path=(*path, "item"))
            return f"array({element}, contains_null={contains_null})"
        if name == "MapType":
            value_contains_null = "True" if bool(type.valueContainsNull) else "False"
            key = self._type(type.keyType, root=root, path=(*path, "key"))
            value = self._type(type.valueType, root=root, path=(*path, "value"))
            return f"map({key}, {value}, value_contains_null={value_contains_null})"
        if name == "StructType":
            nested = self._struct(type, class_name=self._nested_name(root, path), root=root, path=path)
            return f"struct({nested})"
        raise StructureToolError(f"Unsupported Spark type at {'.'.join(path)}: {name}.")

    def _tagged_type(self, physical, descriptor, *, root: str, path: tuple[str, ...]) -> str:
        location = ".".join(path) or "schema"
        if not isinstance(descriptor, dict):
            raise StructureToolError(f"Malformed {METADATA_KEY} metadata at {location}.")
        version = descriptor.get("version")
        if version != DESCRIPTOR_VERSION:
            raise StructureToolError(f"Unsupported {METADATA_KEY} version {version!r} at {location}.")
        kind = descriptor.get("kind")
        if not isinstance(kind, str):
            raise StructureToolError(f"Malformed {METADATA_KEY} descriptor at {location}.")
        expected_keys = {
            "sedona_geometry": {"version", "kind", "srid"},
            "kll_sketch": {"version", "kind", "domain"},
            "theta_sketch": {"version", "kind"},
            "bitmap": {"version", "kind"},
            "hll_sketch": {"version", "kind", "lg_config_k"},
            "array": {"version", "kind", "element"},
            "map": {"version", "kind", "key", "value"},
        }.get(kind)
        if expected_keys is None or set(descriptor) != expected_keys:
            raise StructureToolError(f"Malformed {METADATA_KEY} descriptor at {location}.")
        physical_name = physical.__class__.__name__
        if kind == "sedona_geometry" and physical_name == "GeometryType" and self._is_sedona(physical):
            srid = descriptor.get("srid")
            if srid is not None and (isinstance(srid, bool) or not isinstance(srid, int)):
                raise StructureToolError(f"Invalid Sedona SRID metadata at {location}.")
            return f"sedona.geometry(srid={srid!r})"
        if kind == "kll_sketch" and physical_name == "BinaryType":
            domain = descriptor.get("domain")
            if domain not in {"long", "float", "double"}:
                raise StructureToolError(f"Invalid KLL domain metadata at {location}.")
            return f"kll_sketch({domain}())"
        if kind == "theta_sketch" and physical_name == "BinaryType":
            return "theta_sketch()"
        if kind == "bitmap" and physical_name == "BinaryType":
            return "bitmap()"
        if kind == "hll_sketch" and physical_name == "BinaryType":
            precision = descriptor.get("lg_config_k")
            if isinstance(precision, bool) or not isinstance(precision, int) or not 4 <= precision <= 21:
                raise StructureToolError(f"Invalid HLL precision metadata at {location}.")
            return f"hll_sketch(lg_config_k={precision})"
        if kind == "array" and physical_name == "ArrayType":
            element_descriptor = descriptor.get("element")
            element = (
                self._type(physical.elementType, root=root, path=(*path, "item"))
                if element_descriptor is None
                else self._tagged_type(physical.elementType, element_descriptor, root=root, path=(*path, "item"))
            )
            contains_null = "True" if bool(physical.containsNull) else "False"
            return f"array({element}, contains_null={contains_null})"
        if kind == "map" and physical_name == "MapType":
            key_descriptor = descriptor.get("key")
            value_descriptor = descriptor.get("value")
            key = (
                self._type(physical.keyType, root=root, path=(*path, "key"))
                if key_descriptor is None
                else self._tagged_type(physical.keyType, key_descriptor, root=root, path=(*path, "key"))
            )
            value = (
                self._type(physical.valueType, root=root, path=(*path, "value"))
                if value_descriptor is None
                else self._tagged_type(physical.valueType, value_descriptor, root=root, path=(*path, "value"))
            )
            value_contains_null = "True" if bool(physical.valueContainsNull) else "False"
            return f"map({key}, {value}, value_contains_null={value_contains_null})"
        raise StructureToolError(f"{METADATA_KEY} metadata at {location} conflicts with physical {physical_name}.")

    @staticmethod
    def _is_sedona(type) -> bool:
        return type.__class__.__module__.startswith("sedona.") or "sedona" in type.__class__.__module__.lower()

    @staticmethod
    def _native_srid(type) -> int | None:
        srid = getattr(type, "srid", None)
        if isinstance(srid, bool) or not isinstance(srid, int):
            return None
        return srid

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
