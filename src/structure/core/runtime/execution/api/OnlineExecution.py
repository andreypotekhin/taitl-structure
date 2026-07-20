from structure.core.runtime.execution.commands.RunOnlinePlatformTransform import RunOnlinePlatformTransform


class OnlineExecution:
    def pyspark(self) -> RunOnlinePlatformTransform:
        return RunOnlinePlatformTransform()
