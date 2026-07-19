from structure.core.runtime.execution.commands.RunGeneratedPlatformTransform import RunGeneratedPlatformTransform


class GeneratedExecution:
    @staticmethod
    def pyspark() -> RunGeneratedPlatformTransform:
        return RunGeneratedPlatformTransform()
