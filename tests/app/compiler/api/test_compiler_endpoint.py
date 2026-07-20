from structure import Schema, Transform, input, output, transform
from structure.core.compiler.api import (
    BuildCompilerTraceability,
    ClassifyStreamingCompatibility,
    CompilePlatformTransform,
    Compiler,
    CompileTransform,
)
from structure.core.compiler.artifacts.api.Artifacts import Artifacts
from structure.core.compiler.frontend.api import AnalyzeTransform
from structure.core.compiler.symbolic_execution.api.SymbolicExecution import SymbolicExecution
from structure.core.compiler.symbolic_execution.commands.OpenCompileContext import OpenCompileContext
from structure.core.compiler.symbolic_execution.commands.ReadCompileContext import ReadCompileContext


def test_compiler_endpoint_groups_fresh_command_instances() -> None:
    assert isinstance(Compiler.artifacts, Artifacts)
    assert isinstance(Compiler.symbolic_execution, SymbolicExecution)
    assert isinstance(Compiler.frontend.analyze(), CompileTransform)
    assert isinstance(Compiler.frontend.compile(), CompilePlatformTransform)
    assert isinstance(Compiler.compileability.streaming(), ClassifyStreamingCompatibility)
    assert isinstance(Compiler.traceability.build(), BuildCompilerTraceability)
    assert isinstance(Compiler.symbolic_execution.open(), OpenCompileContext)
    assert isinstance(Compiler.symbolic_execution.current(), ReadCompileContext)

    assert Compiler.frontend.analyze() is not Compiler.frontend.analyze()
    assert Compiler.frontend.compile() is not Compiler.frontend.compile()
    assert Compiler.compileability.streaming() is not Compiler.compileability.streaming()
    assert Compiler.traceability.build() is not Compiler.traceability.build()
    assert Compiler.symbolic_execution.open() is not Compiler.symbolic_execution.open()
    assert Compiler.symbolic_execution.current() is not Compiler.symbolic_execution.current()


def test_frontend_analysis_collects_structure_without_invoking_a_step() -> None:
    class Raw(Schema):
        pass

    class Published(Schema):
        pass

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            raise AssertionError("analysis must not invoke a platform step")

    assert isinstance(Compiler.frontend.analyze(), AnalyzeTransform)
    plan = Compiler.frontend.analyze()(Publish)

    assert [step.name for step in plan.steps] == ["publish"]
    assert plan.steps[0].platform_body is None
    assert not hasattr(plan.outputs[0], "projection")
    assert not hasattr(plan.outputs[0], "operations")
