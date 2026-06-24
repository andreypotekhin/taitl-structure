from structure.app.runtime.api import RuntimeEndpoint, runtime
from structure.app.runtime.execution.generated.api import RunGeneratedPySparkTransform
from structure.app.runtime.execution.online.api import RunOnlinePySparkTransform
from structure.app.runtime.schemas.api import BuildTransformSchemas


def test_runtime_endpoint_groups_fresh_command_instances() -> None:
    assert isinstance(runtime, RuntimeEndpoint)
    assert isinstance(runtime.schemas.build(), BuildTransformSchemas)
    assert isinstance(runtime.execution.online.pyspark(), RunOnlinePySparkTransform)
    assert isinstance(runtime.execution.generated.pyspark(), RunGeneratedPySparkTransform)

    assert runtime.schemas.build() is not runtime.schemas.build()
    assert runtime.execution.online.pyspark() is not runtime.execution.online.pyspark()
    assert runtime.execution.generated.pyspark() is not runtime.execution.generated.pyspark()
