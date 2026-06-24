from structure.app.runtime.execution.api import ExecutionEndpoint
from structure.app.runtime.schemas.api import SchemasEndpoint


class RuntimeEndpoint:

    def __init__(self) -> None:
        self.execution = ExecutionEndpoint()
        self.schemas = SchemasEndpoint()
