from structure.platform.pyspark.execution.online.commands.RunOnlinePySparkTransform import RunOnlinePySparkTransform


class OnlineExecution:

    @staticmethod
    def pyspark() -> RunOnlinePySparkTransform:
        return RunOnlinePySparkTransform()
