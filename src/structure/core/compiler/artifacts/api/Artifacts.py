from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from structure.core.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform
    from structure.core.compiler.artifacts.commands.BuildPluginArtifact import BuildPluginArtifact
    from structure.core.compiler.artifacts.commands.CompareGeneratedFiles import CompareGeneratedFiles
    from structure.core.compiler.artifacts.commands.CompileStructureSources import CompileStructureSources
    from structure.core.compiler.artifacts.commands.GeneratePluginArtifact import GeneratePluginArtifact
    from structure.core.compiler.artifacts.commands.SerializePluginArtifact import SerializePluginArtifact
    from structure.core.compiler.artifacts.commands.WriteGeneratedFiles import WriteGeneratedFiles
    from structure.core.compiler.artifacts.model.CompiledArtifactPool import CompiledArtifactPool


class Artifacts:
    def build(self) -> BuildCompiledTransform:
        from structure.core.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform

        return BuildCompiledTransform()

    def plugin(self, registry) -> BuildPluginArtifact:
        from structure.core.compiler.artifacts.commands.BuildPluginArtifact import BuildPluginArtifact

        return BuildPluginArtifact(registry)

    def sources(self) -> CompileStructureSources:
        from structure.core.compiler.artifacts.commands.CompileStructureSources import CompileStructureSources

        return CompileStructureSources()

    def pool(self) -> CompiledArtifactPool:
        from structure.core.compiler.artifacts.model.CompiledArtifactPool import CompiledArtifactPool

        return CompiledArtifactPool()

    def generate(self, registry) -> GeneratePluginArtifact:
        from structure.core.compiler.artifacts.commands.GeneratePluginArtifact import GeneratePluginArtifact

        return GeneratePluginArtifact(registry)

    def serialize(self, registry) -> SerializePluginArtifact:
        from structure.core.compiler.artifacts.commands.SerializePluginArtifact import SerializePluginArtifact

        return SerializePluginArtifact(registry)

    def compare(self) -> CompareGeneratedFiles:
        from structure.core.compiler.artifacts.commands.CompareGeneratedFiles import CompareGeneratedFiles

        return CompareGeneratedFiles()

    def write(self) -> WriteGeneratedFiles:
        from structure.core.compiler.artifacts.commands.WriteGeneratedFiles import WriteGeneratedFiles

        return WriteGeneratedFiles()
