from collections.abc import Mapping

from structure.plugin.api.v1 import StepAuthoringRequest, StepAuthoringResult, StepAuthoringSession


class Authoring:
    """Explicitly rejects step-body capture because this starter DSL uses decorators."""

    def open_step(self, request: StepAuthoringRequest) -> StepAuthoringSession:
        raise TypeError("The iterable starter DSL declares operations with decorators, not step methods.")

    def result_arguments(self, results: tuple[StepAuthoringResult, ...]) -> tuple[object, ...]:
        return ()

    def rewrite_body(self, body: object, *, frames: Mapping[str, str]) -> object:
        return body
