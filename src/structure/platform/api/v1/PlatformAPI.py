from dataclasses import dataclass

from structure.platform.api.v1.CapabilitiesAPI import CapabilitiesAPI
from structure.platform.api.v1.CompilerAPI import CompilerAPI
from structure.platform.api.v1.ExecutionAPI import ExecutionAPI
from structure.platform.api.v1.GenerationAPI import GenerationAPI
from structure.platform.api.v1.SchemaAPI import SchemaAPI
from structure.platform.api.v1.SerializationAPI import SerializationAPI


@dataclass(frozen=True)
class PlatformAPI:
    schema: SchemaAPI
    compiler: CompilerAPI
    capabilities: CapabilitiesAPI
    executor: ExecutionAPI | None = None
    generator: GenerationAPI | None = None
    serializer: SerializationAPI | None = None
