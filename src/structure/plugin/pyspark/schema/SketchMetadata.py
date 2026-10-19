"""Versioned logical descriptors for opaque sketch values stored as Binary."""

from __future__ import annotations

from collections.abc import Mapping

from structure.plugin.pyspark.dsl.types import (
    ArrayType,
    HllSketchType,
    KllSketchType,
    MapType,
    SketchType,
    ThetaSketchType,
)

METADATA_KEY = "structure.logical_type"
DESCRIPTOR_VERSION = 1


def sketch_descriptor(type) -> dict[str, object] | None:
    if isinstance(type, KllSketchType):
        return {"version": DESCRIPTOR_VERSION, "kind": "kll_sketch", "domain": type.domain}
    if isinstance(type, HllSketchType):
        return {"version": DESCRIPTOR_VERSION, "kind": "hll_sketch", "lg_config_k": type.lg_config_k}
    if isinstance(type, ThetaSketchType):
        return {"version": DESCRIPTOR_VERSION, "kind": "theta_sketch"}
    if isinstance(type, SketchType):
        return {"version": DESCRIPTOR_VERSION, "kind": "bitmap"}
    if isinstance(type, ArrayType):
        element = sketch_descriptor(type.element)
        return None if element is None else {"version": DESCRIPTOR_VERSION, "kind": "array", "element": element}
    if isinstance(type, MapType):
        key = sketch_descriptor(type.key)
        value = sketch_descriptor(type.value)
        return None if key is None and value is None else {
            "version": DESCRIPTOR_VERSION, "kind": "map", "key": key, "value": value
        }
    return None


def field_metadata(type, existing: Mapping[str, object] | None = None, *, path: str = "field") -> dict[str, object]:
    metadata = dict(existing or {})
    expected = sketch_descriptor(type)
    if METADATA_KEY in metadata and metadata[METADATA_KEY] != expected:
        raise ValueError(f"{path} has conflicting {METADATA_KEY} metadata")
    if expected is not None:
        metadata[METADATA_KEY] = expected
    return metadata


def validate_field_metadata(actual: Mapping[str, object] | None, expected_type, *, path: str) -> None:
    """Accept absent tags as caller-declared intent; reject any present conflict."""
    if not actual or METADATA_KEY not in actual:
        return
    descriptor = actual[METADATA_KEY]
    expected = sketch_descriptor(expected_type)
    if not isinstance(descriptor, Mapping):
        raise ValueError(f"{path} has malformed {METADATA_KEY} metadata")
    if descriptor.get("version") != DESCRIPTOR_VERSION:
        raise ValueError(f"{path} has unsupported {METADATA_KEY} version {descriptor.get('version')!r}")
    if dict(descriptor) != expected:
        raise ValueError(f"{path} declares logical type {dict(descriptor)!r}, expected {expected!r}")
