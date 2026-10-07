"""Role base for caller-owned PySpark row writers used by ``foreach``."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast, get_args, get_origin

from structure import Schema
from structure.plugin.api.v1.model.CompilerCodeBoundary import guard_excluded_class

if TYPE_CHECKING:
    from pyspark.sql import Row


class SinkHandoff(Protocol):
    dataframe: object
    schema: type
    output: str
    kind: str
    name: str

RowSchema = TypeVar("RowSchema", bound=Schema)


class Sink(ABC, Generic[RowSchema]):
    """A caller-owned row writer bound to the Structure schema it consumes.

    Subclasses implement ``process`` and may add PySpark streaming ``open`` and
    ``close`` callbacks. The caller constructs the writer and attaches it to a
    named handoff with ``write`` or ``write_stream``.
    """

    _structure_sink_role = True

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        guard_excluded_class(cls, mode="opaque", role="Sink")

    @abstractmethod
    def process(self, row: Row) -> None:
        """Process one PySpark ``Row`` on a Spark worker."""
        raise NotImplementedError

    def write(self, handoff: SinkHandoff) -> None:
        """Apply this writer to a batch row handoff."""
        self._validate_handoff(handoff, kind="row", streaming=False)
        if self._has_streaming_lifecycle():
            raise TypeError("Batch Sink.write(...) cannot use a writer that defines open() or close().")
        cast(Any, handoff.dataframe).foreach(self.process)

    def write_stream(self, handoff: SinkHandoff):
        """Attach this writer to a streaming row handoff and return Spark's writer."""
        self._validate_handoff(handoff, kind="row", streaming=True)
        if self._defines("__call__"):
            raise TypeError("Streaming Sink.write_stream(...) requires a noncallable writer instance.")
        return cast(Any, handoff.dataframe).writeStream.foreach(self)

    def _validate_handoff(self, handoff: SinkHandoff, *, kind: str, streaming: bool) -> None:
        if not all(hasattr(handoff, attribute) for attribute in ("dataframe", "schema", "output", "kind", "name")):
            raise TypeError(f"{type(self).__name__} requires a Structure sink handoff.")
        if handoff.kind != kind:
            raise TypeError(f"{type(self).__name__} requires a {kind} row sink handoff.")
        if bool(getattr(handoff.dataframe, "isStreaming", False)) is not streaming:
            mode = "streaming" if streaming else "batch"
            raise ValueError(f"{type(self).__name__}.write{('_stream' if streaming else '')}(...) requires a {mode} DataFrame.")
        expected = self._schema_argument()
        if expected is not handoff.schema:
            expected_name = expected.__name__ if isinstance(expected, type) else str(expected)
            actual_name = handoff.schema.__name__
            raise TypeError(
                f"{type(self).__name__} consumes {expected_name}, but sink {handoff.name!r} declares {actual_name}."
            )

    def _schema_argument(self) -> object:
        for owner in type(self).__mro__:
            for base in vars(owner).get("__orig_bases__", ()):
                if get_origin(base) is Sink:
                    arguments = get_args(base)
                    if arguments:
                        return arguments[0]
        raise TypeError(f"{type(self).__name__} must inherit Sink[SchemaType].")

    def _has_streaming_lifecycle(self) -> bool:
        return self._defines("open") or self._defines("close")

    def _defines(self, name: str) -> bool:
        return any(name in owner.__dict__ for owner in type(self).__mro__ if owner is not object)
