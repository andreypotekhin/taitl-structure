from structure.plugin.api.v1.model.CompileRequest import CompileRequest
from structure.plugin.api.v1.model.CompilationPurpose import CompilationPurpose
from structure.plugin.api.v1.model.ExecutionRequest import ExecutionRequest
from structure.plugin.api.v1.model.ExplainRequest import ExplainRequest
from structure.plugin.api.v1.model.GenerationRequest import GenerationRequest
from structure.plugin.api.v1.model.GenerationResult import GenerationResult
from structure.plugin.api.v1.model.InputPlan import InputPlan
from structure.plugin.api.v1.model.HookPlan import HookPlan
from structure.plugin.api.v1.model.OutputPlan import OutputPlan
from structure.plugin.api.v1.model.StageOutputPlan import StageOutputPlan
from structure.plugin.api.v1.model.PluginCompilation import PluginCompilation
from structure.plugin.api.v1.model.OptimizationGraph import OptimizationGraph, OptimizationStep
from structure.plugin.api.v1.model.OptimizationReport import OptimizationDecision, OptimizationReport
from structure.plugin.api.v1.model.OptimizationRequest import OptimizationRequest
from structure.plugin.api.v1.model.SchemaInspectionRequest import SchemaInspectionRequest
from structure.plugin.api.v1.model.SchemaValidationRequest import SchemaValidationRequest
from structure.plugin.api.v1.model.StepAuthoringInput import StepAuthoringInput
from structure.plugin.api.v1.model.StepAuthoringSink import StepAuthoringSink
from structure.plugin.api.v1.model.StepAuthoringCapture import StepAuthoringCapture
from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.api.v1.model.StepAuthoringResult import StepAuthoringResult
from structure.plugin.api.v1.model.StepAuthoringSession import StepAuthoringSession
from structure.plugin.api.v1.model.StepInputPlan import StepInputPlan
from structure.plugin.api.v1.model.StepPlan import StepPlan
from structure.plugin.api.v1.model.StepSinkCapture import StepSinkCapture
from structure.plugin.api.v1.model.SinkPlan import SinkPlan
from structure.plugin.api.v1.model.StepResultPlan import StepResultPlan
from structure.plugin.api.v1.model.StreamingAnalysisRequest import StreamingAnalysisRequest
from structure.plugin.api.v1.model.StreamingBoundaryPlan import StreamingBoundaryPlan
from structure.plugin.api.v1.model.StreamingSupport import StreamingSupport
from structure.plugin.api.v1.model.SymbolicContext import SymbolicContext, current_symbolic_context
from structure.plugin.api.v1.model.TraceabilityRequest import TraceabilityRequest
from structure.plugin.api.v1.model.TransformMemberOrigin import TransformMemberOrigin
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.api.v1.model.TransformSchemaRequest import TransformSchemaRequest

# Core owns the construction and lifecycle semantics of these values.  They
# are exposed lazily here so bundled and third-party plugins can consume the
# stable contract without reaching into Core implementation packages.  Keeping
# them lazy avoids a cycle while Core itself constructs structural plans.
_CORE_CONTRACTS = {
    "BackendCapabilities": "structure.core.target.capabilities.model.BackendCapabilities",
    "BackendCapabilityError": "structure.core.target.capabilities.model.BackendCapabilityError",
    "BackendId": "structure.core.target.capabilities.model.BackendId",
    "CapabilityDecision": "structure.core.target.capabilities.model.CapabilityDecision",
    "CapabilityRequirement": "structure.core.target.capabilities.model.CapabilityRequirement",
    "CompilerProvenance": "structure.core.compiler.traceability.model.CompilerProvenance",
    "CompilerTraceability": "structure.core.compiler.traceability.model.CompilerTraceability",
    "DataflowDependency": "structure.core.compiler.traceability.model.DataflowDependency",
    "GeneratedImports": "structure.core.target.capabilities.model.GeneratedImports",
    "GeneratedSchemaClass": "structure.core.tools.logic.model.GeneratedSchemaClass",
    "GeneratedSchemaField": "structure.core.tools.logic.model.GeneratedSchemaField",
    "GeneratedSchemaSource": "structure.core.tools.logic.model.GeneratedSchemaSource",
    "OpaqueBoundary": "structure.core.compiler.traceability.model.OpaqueBoundary",
    "RuntimeDiagnostic": "structure.core.runtime.session.model.RuntimeDiagnostic",
    "StreamingFinding": "structure.core.compiler.compileability.streaming_compatibility.model.StreamingFinding",
    "StreamingReport": "structure.core.compiler.compileability.streaming_compatibility.model.StreamingReport",
    "StreamingStateStage": "structure.core.compiler.compileability.streaming_compatibility.model.StreamingStateStage",
    "StructureRuntimeError": "structure.core.runtime.session.model.StructureRuntimeError",
    "StructureToolError": "structure.core.tools.model.StructureToolError",
    "TransformResult": "structure.core.runtime.session.model.TransformResult",
    "SinkResult": "structure.core.runtime.session.model.SinkResult",
    "StageResult": "structure.core.runtime.session.model.StageResult",
    "TransformSchemas": "structure.core.runtime.schemas.model.TransformSchemas",
    "ValidateSchemaToolRequest": "structure.core.tools.logic.rules.ValidateSchemaToolRequest",
}

__all__ = [
    "OptimizationGraph", "OptimizationStep", "OptimizationDecision", "OptimizationReport", "OptimizationRequest",
    "CompilationPurpose", "CompileRequest", "ExecutionRequest", "ExplainRequest", "GenerationRequest", "GenerationResult", "InputPlan", "PluginCompilation",
    "SchemaInspectionRequest", "SchemaValidationRequest", "StepAuthoringCapture", "StepAuthoringInput", "StepAuthoringRequest", "HookPlan",
    "StepAuthoringResult", "StageResult", "StepAuthoringSession", "StepAuthoringSink", "StepInputPlan", "StepPlan", "StepResultPlan", "StepSinkCapture", "StreamingAnalysisRequest", "StreamingSupport",
    "SymbolicContext", "OutputPlan", "StageOutputPlan", "TraceabilityRequest", "TransformMemberOrigin", "TransformPlan", "TransformSchemaRequest", "StreamingBoundaryPlan", "current_symbolic_context",
    *_CORE_CONTRACTS,
]


def __getattr__(name: str):
    try:
        module_name = _CORE_CONTRACTS[name]
    except KeyError as error:
        raise AttributeError(name) from error
    from importlib import import_module

    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value
