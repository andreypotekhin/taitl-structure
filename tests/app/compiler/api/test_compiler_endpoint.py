from structure.core.compiler.api import (
    BuildCompilerTraceability,
    ClassifyStreamingCompatibility,
    CompilePlatformTransform,
    Compiler,
    CompileTransform,
)
from structure.core.compiler.artifacts.api.Artifacts import Artifacts
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
