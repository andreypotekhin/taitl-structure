from structure.core.runtime.execution.commands.RunGeneratedPluginTransform import RunGeneratedPluginTransform


class GeneratedExecution:
    def pyspark(self) -> RunGeneratedPluginTransform:
        return RunGeneratedPluginTransform()
