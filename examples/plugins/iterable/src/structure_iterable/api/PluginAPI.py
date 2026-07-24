from structure.plugin.api.v1 import PluginAPI as PluginAPIV1

from ..authoring import Authoring
from ..capabilities import Capabilities
from ..compiler import Compiler
from ..execution import Execution
from ..schema import Schema
from ..serialization import Serialization


class PluginAPI:
    """Assembles the v1 façade from the plugin's focused applications."""

    def create(self) -> PluginAPIV1:
        return PluginAPIV1(
            schema=Schema(),
            authoring=Authoring(),
            compiler=Compiler(),
            capabilities=Capabilities(),
            executor=Execution(),
            serializer=Serialization(),
        )
