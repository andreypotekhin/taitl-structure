from structure.core.runtime.api import Runtime
from structure.core.runtime.execution.api import RunGeneratedPluginTransform, RunOnlinePluginTransform
from structure.core.runtime.schemas.api import BuildTransformSchemas


def test_runtime_endpoint_groups_fresh_command_instances() -> None:
    assert isinstance(Runtime.schemas.build(), BuildTransformSchemas)
    assert isinstance(Runtime.execution.online.pyspark(), RunOnlinePluginTransform)
    assert isinstance(Runtime.execution.generated.pyspark(), RunGeneratedPluginTransform)

    assert Runtime.schemas.build() is not Runtime.schemas.build()
    assert Runtime.execution.online.pyspark() is not Runtime.execution.online.pyspark()
    assert Runtime.execution.generated.pyspark() is not Runtime.execution.generated.pyspark()
