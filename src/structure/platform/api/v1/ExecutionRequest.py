from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionRequest:
    payload: object
    runtime: object
    invocation: object | None = None
    mode: str | None = None
    semantic_fingerprint: str | None = None
