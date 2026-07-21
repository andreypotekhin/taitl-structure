from dataclasses import dataclass

from structure.plugin.api.v1.api.AnalysisAPI import AnalysisAPI
from structure.plugin.api.v1.api.AuthoringAPI import AuthoringAPI
from structure.plugin.api.v1.api.CapabilitiesAPI import CapabilitiesAPI
from structure.plugin.api.v1.api.CompilerAPI import CompilerAPI
from structure.plugin.api.v1.api.ExecutionAPI import ExecutionAPI
from structure.plugin.api.v1.api.ExplainAPI import ExplainAPI
from structure.plugin.api.v1.api.GenerationAPI import GenerationAPI
from structure.plugin.api.v1.api.SchemaAPI import SchemaAPI
from structure.plugin.api.v1.api.SerializationAPI import SerializationAPI


@dataclass(frozen=True)
class PluginAPIV1:
    schema: SchemaAPI
    compiler: CompilerAPI
    capabilities: CapabilitiesAPI
    authoring: AuthoringAPI
    executor: ExecutionAPI | None = None
    generator: GenerationAPI | None = None
    serializer: SerializationAPI | None = None
    explainer: ExplainAPI | None = None
    analysis: AnalysisAPI | None = None
