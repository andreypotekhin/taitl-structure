from pathlib import Path

from structure.lib.cross.errors import SourceSpan, build_source_span


class BuildCompilerDiagnosticSource:
    def __call__(
        self,
        transform_class: type | None,
        member: str | None = None,
        *,
        project_root: Path | None = None,
        label: str = "",
    ) -> SourceSpan | None:
        if transform_class is None:
            return None
        value = getattr(transform_class, member, transform_class) if member else transform_class
        return build_source_span(value, project_root=project_root, label=label)
