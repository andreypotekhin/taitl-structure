from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import uuid4


@contextmanager
def temporary_view_boundary(spark, frame) -> Iterator[Any]:
    """Expose a lazy DataFrame through a unique session-scoped temporary view."""
    name = f"_structure_boundary_{uuid4().hex}"
    frame.createOrReplaceTempView(name)
    try:
        yield spark.table(name)
    finally:
        try:
            spark.catalog.dropTempView(name)
        except Exception:
            # Cleanup is best effort: Spark may already have removed the view
            # when the owning session is being torn down.
            pass
