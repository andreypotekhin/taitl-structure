from structure.app.runtime.execution.online.logic.actions.RunOnlinePySparkTransform import RunOnlinePySparkTransform


class OnlineExecutionEndpoint:

    def pyspark(self) -> RunOnlinePySparkTransform:
        return RunOnlinePySparkTransform()
