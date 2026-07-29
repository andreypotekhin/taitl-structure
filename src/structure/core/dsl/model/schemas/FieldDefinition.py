"""Final schema field metadata used by Structure and target plugins."""

from __future__ import annotations

from builtins import type as class_type
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Mapping


@dataclass(frozen=True)
class AnnotationType:
    """Sentinel for fields declared by annotation and resolved by a plugin."""

    name: str = "__annotation__"


ANNOTATION_TYPE = AnnotationType()


@dataclass(frozen=True)
class FieldDefinition:
    """A finalized field on a ``Schema`` class.

    The definition keeps both the public Structure field name and the physical
    target column alias.  Target plugins use it to materialize schemas, validate
    nullability, and generate code.
    """

    name: str
    type: Any
    hint: object | None = None
    nullable: bool = True
    alias: str | None = None
    metadata: Mapping[str, object] = dataclass_field(default_factory=dict)
    description: str | None = None
    validator: Callable[[class_type, Mapping[str, "FieldDefinition"]], None] | None = dataclass_field(
        default=None, repr=False, compare=False
    )

    @property
    def column(self) -> str:
        """Return the target column name visible to generated code."""
        return self.alias or self.name
