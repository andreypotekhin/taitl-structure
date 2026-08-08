"""Private shared support for transient text materialization lanes."""

from typing import Any

from structure import Transform, special
from structure.plugin.pyspark import types


class _TextMaterializer(Transform):
    """Shared private UDF for canonical document-code-point span extraction."""

    @special(type="udf", return_type=types.string(), nullable=False)
    def canonical_span(content: Any, start: Any, end: Any) -> str | None:
        """Extract a half-open span from canonicalized Unicode document text."""
        import re

        if content is None or start is None or end is None:
            return None
        canonical = re.sub(r"\r\n?", "\n", content)
        return canonical[int(start) : int(end)]
