from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from structure.app.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform


class Artifacts:

    @staticmethod
    def build() -> BuildCompiledTransform:
        from structure.app.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform

        return BuildCompiledTransform()
