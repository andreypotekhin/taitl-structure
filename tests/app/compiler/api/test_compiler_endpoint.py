from structure.app.compiler.api import (
    BuildCompilerTraceability,
    ClassifyStreamingCompatibility,
    CompileTransform,
    compiler,
)


def test_compiler_endpoint_groups_fresh_command_instances() -> None:
    assert isinstance(compiler.frontend.compile(), CompileTransform)
    assert isinstance(compiler.compileability.streaming(), ClassifyStreamingCompatibility)
    assert isinstance(compiler.traceability.build(), BuildCompilerTraceability)

    assert compiler.frontend.compile() is not compiler.frontend.compile()
    assert compiler.compileability.streaming() is not compiler.compileability.streaming()
    assert compiler.traceability.build() is not compiler.traceability.build()
