"""Base class for Structure row schemas.

Schemas are the shared contract between user-authored transforms and target
plugins.  A schema class declares the logical fields a transform reads or
writes; plugins then attach target-specific field types and validation.
"""

from __future__ import annotations

from typing import ClassVar, get_origin, get_type_hints

from structure.core.dsl.model.schemas.FieldDeclaration import FieldDeclaration
from structure.core.dsl.model.schemas.FieldDefinition import ANNOTATION_TYPE, FieldDefinition


class Schema:
    """A typed row shape used by Structure transforms.

    Subclasses declare fields either through plugin field factories, such as
    ``structure.plugin.pyspark.string()``, or through annotations that a plugin
    can later resolve.  Schema instances are lightweight value objects used for
    expected results, projections, and transform invocation tests.

    Args:
        **values: Field values keyed by declared Structure field name.

    Raises:
        TypeError: Unknown fields are supplied, mixed plugin field declarations
            appear in one schema, or inherited ``base(...)`` sources cannot
            provide required base fields.

    Example:
        class Order(Schema):
            id = pyspark.string(nullable=False)
            total = pyspark.decimal(12, 2)

        expected = Order(id="A-1", total=Decimal("10.50"))
    """

    _structure_fields: dict[str, FieldDefinition] = {}
    _structure_local_fields: dict[str, FieldDefinition] = {}
    _structure_schema_bases: tuple[type["Schema"], ...] = ()

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        fields: dict[str, FieldDefinition] = {}
        for base in cls.__bases__:
            fields.update(getattr(base, "_structure_fields", {}))

        local_fields: dict[str, FieldDefinition] = {}
        annotations = cls.__dict__.get("__annotations__", {})
        hints = cls._local_hints(annotations)
        for name, value in cls.__dict__.items():
            if isinstance(value, FieldDeclaration):
                definition = value.definition(name, hints.get(name))
                local_fields[name] = definition
                fields[name] = definition

        for name, hint in hints.items():
            if name not in cls.__dict__:
                definition = FieldDefinition(name=name, type=ANNOTATION_TYPE, hint=hint)
                local_fields[name] = definition
                fields[name] = definition

        cls._validate_fields(fields)
        local_fields = {name: fields[name] for name in local_fields}
        cls._structure_fields = fields
        cls._structure_local_fields = local_fields
        cls._structure_schema_bases = tuple(
            base for base in cls.__bases__ if isinstance(base, type) and issubclass(base, Schema) and base is not Schema
        )

    def __init__(self, **values: object) -> None:
        unknown = set(values) - set(self._structure_fields)
        if unknown:
            allowed = ", ".join(self._structure_fields)
            raise TypeError(
                f"{type(self).__name__} got unknown field(s): {', '.join(sorted(unknown))}. Allowed: {allowed}"
            )
        self._structure_values = dict(values)

    @classmethod
    def base(cls, *sources: object):
        """Build a schema instance by copying direct base-schema fields.

        Args:
            *sources: One source row for each direct schema base.

        Returns:
            A completed schema instance when all fields are available, otherwise
            a callable that accepts overrides for local fields.

        Example:
            class OrderCore(Schema):
                id = pyspark.string()

            class PublishedOrder(OrderCore):
                published_at = pyspark.timestamp()

            row = PublishedOrder.base(order)(published_at=event_time)
        """
        builder = _SchemaBaseBuilder(cls, sources)
        return builder.materialize() if builder.complete else builder

    @classmethod
    def project(cls, *sources: object):
        """Project one or more row sources into this schema during step authoring.

        This is the target-neutral form of plugin projection helpers.  In the
        PySpark DSL it lowers through ``pyspark.project(source, TargetSchema)``.

        Args:
            *sources: Row scopes or schema instances that can provide fields by
                Structure name.

        Returns:
            A plugin-owned symbolic projection for this schema.

        Example:
            @step(input=orders, output=published)
            def publish(self, order):
                return PublishedOrder.project(order)
        """
        from structure.plugin.api.v1.model import current_symbolic_context

        context = current_symbolic_context()
        project = None if context is None else getattr(context, "project", None)
        if not callable(project):
            raise RuntimeError(f"{cls.__name__}.project(...) can only be used while a plugin authors a Structure step")
        if not sources:
            raise TypeError(f"{cls.__name__}.project(...) requires at least one source row")
        return project(*sources, target=cls)

    @classmethod
    def _base_values(cls, sources: tuple[object, ...]) -> dict[str, object]:
        bases = cls._structure_schema_bases
        if not bases:
            raise TypeError(f"{cls.__name__}.base(...) requires a schema that directly inherits from another Schema")
        if len(sources) != len(bases):
            raise TypeError(
                f"{cls.__name__}.base(...) requires {len(bases)} source row(s), one for each direct schema base"
            )
        values: dict[str, object] = {}
        for base, source in zip(bases, sources, strict=True):
            source_schema = cls._source_schema(source)
            if source_schema is None or not set(base._structure_fields).issubset(source_schema._structure_fields):
                actual = type(source).__name__ if source_schema is None else source_schema.__name__
                raise TypeError(
                    f"{cls.__name__}.base(...) source for {base.__name__} must provide every "
                    f"{base.__name__} field, but {actual} does not"
                )
            for field in base._structure_fields:
                if field in cls._structure_local_fields or field in values:
                    continue
                value = cls._field_value(source, field)
                if value is not _MISSING:
                    values[field] = value
        return values

    @classmethod
    def _project_values(cls, sources: tuple[object, ...], *, skip: set[str] | None = None) -> dict[str, object]:
        skip = set() if skip is None else skip
        values: dict[str, object] = {}
        for field in cls._structure_fields:
            if field in skip:
                continue
            providers = [source for source in sources if cls._source_has_field(source, field)]
            if len(providers) == 1:
                value = cls._field_value(providers[0], field)
                if value is not _MISSING:
                    values[field] = value
        return values

    @staticmethod
    def _source_schema(source: object) -> type["Schema"] | None:
        if isinstance(source, Schema):
            return type(source)
        schema = getattr(source, "_structure_scope_schema", None)
        return schema if isinstance(schema, type) and issubclass(schema, Schema) else None

    @classmethod
    def _source_has_field(cls, source: object, field: str) -> bool:
        schema = cls._source_schema(source)
        return schema is not None and field in schema._structure_fields

    @staticmethod
    def _field_value(source: object, field: str) -> object:
        if isinstance(source, Schema):
            return source._structure_values.get(field, _MISSING)

        try:
            return getattr(source, field)
        except AttributeError:
            return _MISSING

    @classmethod
    def _validate_fields(cls, fields: dict[str, FieldDefinition]) -> None:
        validators = {field.validator for field in fields.values() if field.validator is not None}
        if len(validators) > 1:
            raise TypeError(
                f"{cls.__name__} combines field declarations from multiple plugins. "
                "Use one plugin's field DSL for each Schema."
            )
        if validators:
            validators.pop()(cls, fields)

    @classmethod
    def _local_hints(cls, annotations: object) -> dict[str, object]:
        if not isinstance(annotations, dict):
            return {}
        try:
            resolved = get_type_hints(cls)
        except (NameError, TypeError):
            resolved = annotations
        return {
            name: hint for name, hint in resolved.items() if name in annotations and get_origin(hint) is not ClassVar
        }


_MISSING = object()


class _SchemaBaseBuilder:

    def __init__(
        self,
        target: type[Schema],
        base_sources: tuple[object, ...],
        project_sources: tuple[object, ...] = (),
    ) -> None:
        self._target = target
        self._base_sources = base_sources
        self._project_sources = project_sources

    @property
    def complete(self) -> bool:
        return set(self._values()) == set(self._target._structure_fields)

    def project(self, *sources: object) -> "_SchemaBaseBuilder":
        if not sources:
            raise TypeError(f"{self._target.__name__}.base(...).project(...) requires at least one source row")
        return _SchemaBaseBuilder(self._target, self._base_sources, self._project_sources + sources)

    def __call__(self, **overrides: object) -> Schema:
        values = self._values()
        values.update(overrides)
        return self._target(**values)

    def materialize(self) -> Schema:
        return self()

    def _values(self) -> dict[str, object]:
        values = self._target._base_values(self._base_sources)
        values.update(self._target._project_values(self._project_sources, skip=set(values)))
        return values
