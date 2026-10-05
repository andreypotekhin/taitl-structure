"""Role base for caller-owned PySpark row writers used by ``foreach``."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from structure.plugin.api.v1.model.CompilerCodeBoundary import guard_excluded_class

if TYPE_CHECKING:
    from pyspark.sql import Row


class Sink(ABC):
    """A caller-owned row writer whose methods are opaque to Structure steps.

    Subclasses implement ``process`` and may add PySpark streaming ``open`` and
    ``close`` callbacks. Structure passes the class through a named sink
    handoff; it never constructs or invokes the writer.
    """

    _structure_sink_role = True

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        guard_excluded_class(cls, mode="opaque", role="Sink")

    @abstractmethod
    def process(self, row: Row) -> None:
        """Process one PySpark ``Row`` on a Spark worker."""
        raise NotImplementedError
