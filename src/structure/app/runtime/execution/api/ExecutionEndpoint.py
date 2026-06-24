from structure.app.runtime.execution.generated.api import GeneratedExecutionEndpoint
from structure.app.runtime.execution.online.api import OnlineExecutionEndpoint


class ExecutionEndpoint:

    def __init__(self) -> None:
        self.generated = GeneratedExecutionEndpoint()
        self.online = OnlineExecutionEndpoint()
