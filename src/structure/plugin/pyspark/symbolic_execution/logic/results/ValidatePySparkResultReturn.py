from typing import Callable, cast

from structure.dsl import Schema
from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.pyspark.dsl.Projection import Projection


class ValidatePySparkResultReturn:
    """Validate a captured step's return shape against its declared result schemas."""

    def __init__(self, request: StepAuthoringRequest, raise_error: Callable[[str, str, str], None]) -> None:
        self._request = request
        self._raise = raise_error

    def __call__(self, value: object) -> tuple[Schema | Projection, ...]:
        results = self._request.results
        if len(results) == 1:
            return (cast(Schema | Projection, value),)
        if not isinstance(value, tuple) or len(value) != len(results):
            actual = len(value) if isinstance(value, tuple) else type(value).__name__
            self._raise(
                "DSL-E0402",
                f"{self._request.name} must return {len(results)} schema values; got {actual}.",
                "Return a tuple whose values match the fixed tuple annotation in order.",
            )
        values = cast(tuple[object, ...], value)
        if any(isinstance(item, Projection) for item in values):
            self._raise(
                "DSL-E0402",
                f"{self._request.name} uses project(...) in a multi-output return.",
                "Return explicit schema instances for tuple-returning step methods.",
            )
        return cast(tuple[Schema | Projection, ...], values)
