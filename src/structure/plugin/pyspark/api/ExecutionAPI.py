from typing import Any, cast

from structure.plugin.api.v1 import ExecutionAPI as ExecutionAPIV1
from structure.plugin.api.v1 import ExecutionRequest
from structure.plugin.pyspark.api.PySpark import PySpark


class ExecutionAPI(ExecutionAPIV1):
    def validate_runtime(self, request: ExecutionRequest) -> None:
        session = request.runtime
        PySpark.execution.validate_runtime(
            session,
            transform=(f"{type(request.invocation).__module__}.{type(request.invocation).__name__}"
                       if request.invocation is not None else ""),
            execution_mode=request.mode or "online",
        )

    def execute(self, request: ExecutionRequest) -> object:
        if request.invocation is None or request.mode is None:
            raise ValueError("PySpark execution requires an invocation and execution mode.")
        if getattr(request.runtime, "online_executor", None) is None:
            self.validate_runtime(request)
        if request.mode == "online":
            return PySpark.execution.online()(
                cast(Any, request.invocation), cast(Any, request.payload), session=request.runtime
            )
        if request.mode == "generated":
            return PySpark.execution.generated()(
                cast(Any, request.invocation),
                cast(Any, request.payload),
                session=request.runtime,
                semantic_fingerprint=request.semantic_fingerprint,
            )
        raise ValueError(f"PySpark does not support execution mode {request.mode!r}.")
