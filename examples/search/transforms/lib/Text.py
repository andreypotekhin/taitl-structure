"""Shared text helpers for transient Search materialization lanes."""

from typing import Any

from structure import special
from structure.plugin.pyspark import types


class Text:
    """Compiler-visible helpers for working with document text."""

    @special(type="udf", return_type=types.string(), nullable=False)
    def span(content: Any, start: Any, end: Any) -> str | None:
        """Extract a half-open span from canonicalized Unicode document text."""
        import re

        if content is None or start is None or end is None:
            return None
        canonical = re.sub(r"\r\n?", "\n", content)
        return canonical[int(start) : int(end)]
