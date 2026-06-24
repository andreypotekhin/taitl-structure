from structure.app.compiler.api import (
    BuildCompilerTraceability,
    ClassifyStreamingCompatibility,
    Compiler,
    CompileTransform,
)


def test_compiler_endpoint_groups_fresh_command_instances() -> None:
    assert isinstance(Compiler.frontend.compile(), CompileTransform)
    assert isinstance(Compiler.compileability.streaming(), ClassifyStreamingCompatibility)
    assert isinstance(Compiler.traceability.build(), BuildCompilerTraceability)

    assert Compiler.frontend.compile() is not Compiler.frontend.compile()
    assert Compiler.compileability.streaming() is not Compiler.compileability.streaming()
    assert Compiler.traceability.build() is not Compiler.traceability.build()
