from structure.core.runtime.execution.commands.RunOnlinePluginTransform import RunOnlinePluginTransform


class OnlineExecution:
    def pyspark(self) -> RunOnlinePluginTransform:
        return RunOnlinePluginTransform()
