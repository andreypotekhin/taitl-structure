from typing import Any, cast

from structure.platform.api.v1 import ExecutionAPI, ExecutionRequest
from structure.platform.pyspark.execution.generated.commands.RunGeneratedPySparkTransform import (
    RunGeneratedPySparkTransform,
)
from structure.platform.pyspark.execution.online.commands.RunOnlinePySparkTransform import RunOnlinePySparkTransform


class Execution(ExecutionAPI):
    def execute(self, request: ExecutionRequest) -> object:
        if request.invocation is None or request.mode is None:
            raise ValueError("PySpark execution requires an invocation and execution mode.")
        if request.mode == "online":
            return RunOnlinePySparkTransform()(
                cast(Any, request.invocation), cast(Any, request.payload), session=request.runtime
            )
        if request.mode == "generated":
            return RunGeneratedPySparkTransform()(
                cast(Any, request.invocation),
                cast(Any, request.payload),
                session=request.runtime,
                semantic_fingerprint=request.semantic_fingerprint,
            )
        raise ValueError(f"PySpark does not support execution mode {request.mode!r}.")
