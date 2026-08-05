from structure.plugin.api.v1 import PluginAPI as PluginAPIV1
from structure.plugin.pyspark.api.AnalysisAPI import AnalysisAPI
from structure.plugin.pyspark.api.AuthoringAPI import AuthoringAPI
from structure.plugin.pyspark.api.CapabilitiesAPI import CapabilitiesAPI
from structure.plugin.pyspark.api.CompilerAPI import CompilerAPI
from structure.plugin.pyspark.api.ExecutionAPI import ExecutionAPI
from structure.plugin.pyspark.api.ExplainAPI import ExplainAPI
from structure.plugin.pyspark.api.GenerationAPI import GenerationAPI
from structure.plugin.pyspark.api.SchemaAPI import SchemaAPI
from structure.plugin.pyspark.api.SemanticDefaultsAPI import SemanticDefaultsAPI


class PluginAPI:
    def create(self) -> PluginAPIV1:
        return PluginAPIV1(
            schema=SchemaAPI(),
            compiler=CompilerAPI(),
            capabilities=CapabilitiesAPI(),
            authoring=AuthoringAPI(),
            executor=ExecutionAPI(),
            generator=GenerationAPI(),
            explainer=ExplainAPI(),
            analysis=AnalysisAPI(),
            semantic_defaults=SemanticDefaultsAPI(),
        )
