from __future__ import annotations

from structure.lib.cross.errors.Diagnostic import Diagnostic


class RenderDiagnostic:

    def __call__(self, diagnostic: Diagnostic, *, kind: str = "Diagnostic") -> str:
        lines = [
            f"{kind} {diagnostic.code}: {diagnostic.title}",
            "",
            "Severity:",
            f"  {diagnostic.severity}",
        ]
        if diagnostic.context:
            lines.extend(["", "Context:"])
            lines.extend(f"  {key}: {value}" for key, value in diagnostic.context.items())
        if diagnostic.source:
            lines.extend(["", "Source:", f"  {diagnostic.source}"])
        lines.extend(["", "Problem:", f"  {diagnostic.problem_text()}"])
        if diagnostic.why:
            lines.extend(["", "Why:", f"  {diagnostic.why}"])
        lines.extend(["", "Use:", f"  {diagnostic.use_text()}", "", f"See {diagnostic.docs}"])
        return "\n".join(lines)


render_diagnostic = RenderDiagnostic()
