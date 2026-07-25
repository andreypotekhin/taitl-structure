from structure.plugin.api.v1 import PluginAPI as PluginAPIV1

from ..authoring import Authoring
from ..capabilities import Capabilities
from ..compiler import Compiler
from ..execution import Execution
from ..generation import Generation
from ..schema import Schema
from ..serialization import Serialization


class PluginAPI(PluginAPIV1):
    """The concrete v1 façade assembled from the plugin's focused applications."""

    def __init__(self) -> None:
        super().__init__(
            schema=Schema(),
            authoring=Authoring(),
            compiler=Compiler(),
            capabilities=Capabilities(),
            executor=Execution(),
            generator=Generation(),
            serializer=Serialization(),
        )
