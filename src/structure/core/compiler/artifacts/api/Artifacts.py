from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from structure.core.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform
    from structure.core.compiler.artifacts.commands.BuildPlatformArtifact import BuildPlatformArtifact
    from structure.core.compiler.artifacts.commands.CompareGeneratedFiles import CompareGeneratedFiles
    from structure.core.compiler.artifacts.commands.CompileStructureSources import CompileStructureSources
    from structure.core.compiler.artifacts.commands.GeneratePlatformArtifact import GeneratePlatformArtifact
    from structure.core.compiler.artifacts.commands.SerializePlatformArtifact import SerializePlatformArtifact
    from structure.core.compiler.artifacts.commands.WriteGeneratedFiles import WriteGeneratedFiles
    from structure.core.compiler.artifacts.model.CompiledArtifactPool import CompiledArtifactPool


class Artifacts:
    def build(self) -> BuildCompiledTransform:
        from structure.core.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform

        return BuildCompiledTransform()

    def platform(self, registry) -> BuildPlatformArtifact:
        from structure.core.compiler.artifacts.commands.BuildPlatformArtifact import BuildPlatformArtifact

        return BuildPlatformArtifact(registry)

    def sources(self) -> CompileStructureSources:
        from structure.core.compiler.artifacts.commands.CompileStructureSources import CompileStructureSources

        return CompileStructureSources()

    def pool(self) -> CompiledArtifactPool:
        from structure.core.compiler.artifacts.model.CompiledArtifactPool import CompiledArtifactPool

        return CompiledArtifactPool()

    def generate(self, registry) -> GeneratePlatformArtifact:
        from structure.core.compiler.artifacts.commands.GeneratePlatformArtifact import GeneratePlatformArtifact

        return GeneratePlatformArtifact(registry)

    def serialize(self, registry) -> SerializePlatformArtifact:
        from structure.core.compiler.artifacts.commands.SerializePlatformArtifact import SerializePlatformArtifact

        return SerializePlatformArtifact(registry)

    def compare(self) -> CompareGeneratedFiles:
        from structure.core.compiler.artifacts.commands.CompareGeneratedFiles import CompareGeneratedFiles

        return CompareGeneratedFiles()

    def write(self) -> WriteGeneratedFiles:
        from structure.core.compiler.artifacts.commands.WriteGeneratedFiles import WriteGeneratedFiles

        return WriteGeneratedFiles()
