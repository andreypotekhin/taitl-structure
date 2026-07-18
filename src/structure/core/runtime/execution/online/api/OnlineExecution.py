from structure.core.runtime.execution.online.commands.RunOnlinePySparkTransform import RunOnlinePySparkTransform


class OnlineExecution:

    @staticmethod
    def pyspark() -> RunOnlinePySparkTransform:
        return RunOnlinePySparkTransform()
