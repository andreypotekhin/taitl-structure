from structure.app.runtime.execution.online.api.OnlineExecutionEndpoint import OnlineExecutionEndpoint
from structure.app.runtime.execution.online.logic.actions.RunOnlinePySparkTransform import RunOnlinePySparkTransform

online = OnlineExecutionEndpoint()
run_online_pyspark_transform = RunOnlinePySparkTransform()

__all__ = [
    "OnlineExecutionEndpoint",
    "RunOnlinePySparkTransform",
    "online",
    "run_online_pyspark_transform",
]
