from structure.core.runtime.execution.commands.RunOnlinePlatformTransform import RunOnlinePlatformTransform


class OnlineExecution:
    @staticmethod
    def pyspark() -> RunOnlinePlatformTransform:
        return RunOnlinePlatformTransform()
