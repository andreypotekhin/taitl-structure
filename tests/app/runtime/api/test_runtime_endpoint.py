from structure.core.runtime.api import Runtime
from structure.core.runtime.execution.generated.api import RunGeneratedPySparkTransform
from structure.core.runtime.execution.online.api import RunOnlinePySparkTransform
from structure.core.runtime.schemas.api import BuildTransformSchemas


def test_runtime_endpoint_groups_fresh_command_instances() -> None:
    assert isinstance(Runtime.schemas.build(), BuildTransformSchemas)
    assert isinstance(Runtime.execution.online.pyspark(), RunOnlinePySparkTransform)
    assert isinstance(Runtime.execution.generated.pyspark(), RunGeneratedPySparkTransform)

    assert Runtime.schemas.build() is not Runtime.schemas.build()
    assert Runtime.execution.online.pyspark() is not Runtime.execution.online.pyspark()
    assert Runtime.execution.generated.pyspark() is not Runtime.execution.generated.pyspark()
