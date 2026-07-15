from __future__ import annotations

from structure.lib.cross.errors.Diagnostic import Diagnostic
from structure.lib.cross.errors.SourceSpan import SourceSpan


class RenderDiagnosticSource:

    def __call__(self, diagnostic: Diagnostic) -> list[str]:
        if diagnostic.primary_span is None:
            return [f"  {diagnostic.source}"] if diagnostic.source else []
        lines = self._span(diagnostic.primary_span, marker="-->")
        for span in diagnostic.related_spans:
            lines.extend(["", *self._span(span, marker=":::")])
        return lines

    def _span(self, span: SourceSpan, *, marker: str) -> list[str]:
        lines = [f"  {marker} {span.path}:{span.start_line}:{span.start_column}"]
        if span.excerpt is None:
            return lines
        lines.append("   |")
        for offset, source in enumerate(span.excerpt.lines[:5]):
            number = span.excerpt.first_line + offset
            text = source.expandtabs(4)
            lines.append(f"  {number} | {text}")
            if number == span.start_line:
                lines.append(self._marker(span, source, number))
        return lines

    def _marker(self, span: SourceSpan, source: str, number: int) -> str:
        start = len(source[: span.start_column - 1].expandtabs(4))
        length = max(1, span.end_column - span.start_column)
        available = max(1, len(source.expandtabs(4)) - start)
        carets = "^" * min(length, available)
        label = f" {span.label}" if span.label else ""
        return f"  {' ' * len(str(number))} | {' ' * start}{carets}{label}"


render_diagnostic_source = RenderDiagnosticSource()
