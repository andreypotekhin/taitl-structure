"""Transform base class for hierarchies that require streaming-compatible steps."""

from structure.core.dsl.model.transforms.Transform import Transform


class StreamingTransform(Transform):
    """A transform hierarchy whose descendants must remain streaming-compatible.

    Every concrete step is checked against the streaming compatibility contract, even when the
    transform currently receives batch DataFrames. The class does not declare streaming input
    lineage or manage a streaming query; declare streaming inputs with ``input(..., streaming=True)``.

    Use ``@transform(streaming=True)`` instead when the requirement applies to one class only.
    """
