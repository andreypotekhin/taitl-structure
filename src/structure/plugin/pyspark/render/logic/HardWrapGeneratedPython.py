from __future__ import annotations

import ast
import textwrap


class HardWrapGeneratedPython:

    def __call__(self, source: str, *, width: int) -> str:
        if width <= 0:
            return source
        formatted = self._black(source, width=width)
        if formatted is not None:
            source = formatted
        valid = source
        for _ in range(3):
            wrapped = "\n".join(line.rstrip() for line in self._lines(source.splitlines(), width=width)) + "\n"
            if not self._parseable(wrapped):
                return valid
            valid = wrapped
            if wrapped == source or all(len(line) <= width for line in wrapped.splitlines()):
                return wrapped
            source = wrapped
        return source

    def _black(self, source: str, *, width: int) -> str | None:
        try:
            import black
        except ImportError:
            return None
        try:
            return black.format_str(
                source,
                mode=black.Mode(line_length=width, string_normalization=False),
            )
        except black.InvalidInput:
            return None

    def _parseable(self, source: str) -> bool:
        try:
            ast.parse(source)
        except SyntaxError:
            return False
        return True

    def _lines(self, lines: list[str], *, width: int) -> list[str]:
        wrapped: list[str] = []
        index = 0
        while index < len(lines):
            line = lines[index]
            if not self._wrap_candidate(line, width=width):
                wrapped.append(line)
                index += 1
                continue
            statement = [line]
            balance = self._balance(line)
            index += 1
            while balance > 0 and index < len(lines):
                statement.append(lines[index])
                balance += self._balance(lines[index])
                index += 1
            wrapped.extend(self._line(self._join_statement(statement), width=width).splitlines())
        return wrapped

    def _line(self, line: str, *, width: int) -> str:
        if len(line) <= width:
            return line
        if line.startswith("from ") and " import " in line:
            return self._import(line)
        indent = self._indent(line)
        content = line[len(indent) :]
        literal = self._string_literal(indent, content, width=width)
        if literal is not None:
            return literal
        for marker in (" = ", "return "):
            prefix, expression = self._split(content, marker)
            if expression:
                return self._expression(indent, prefix, expression, width=width)
        return self._expression(indent, "", content, width=width)

    def _wrap_candidate(self, line: str, *, width: int) -> bool:
        if len(line) <= width:
            return False
        content = line.lstrip()
        return not content.startswith(("#", "class ", "def ", "@"))

    def _join_statement(self, lines: list[str]) -> str:
        if len(lines) == 1:
            return lines[0]
        indent = self._indent(lines[0])
        return indent + " ".join(line.strip() for line in lines)

    def _balance(self, line: str) -> int:
        balance = 0
        quote = ""
        escaped = False
        for character in line:
            if quote:
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == quote:
                    quote = ""
                continue
            if character in ("'", '"'):
                quote = character
            elif character in "([{":
                balance += 1
            elif character in ")]}":
                balance -= 1
        return balance

    def _import(self, line: str) -> str:
        source, names = line.split(" import ", 1)
        indent = self._indent(line)
        return "\n".join(
            [
                f"{source} import (",
                *(f"{indent}    {name.strip()}," for name in names.split(",")),
                f"{indent})",
            ]
        )

    def _expression(self, indent: str, prefix: str, expression: str, *, width: int) -> str:
        suffix = ""
        if not prefix and expression.rstrip().endswith(","):
            suffix = ","
            expression = expression.rstrip()[:-1]
        expression_indent = f"{indent}    "
        rendered = self._render_expression(expression, indent=expression_indent, width=width)
        return "\n".join([f"{indent}{prefix}(", rendered, f"{indent}){suffix}"])

    def _string_literal(self, indent: str, content: str, *, width: int) -> str | None:
        extra = content[: len(content) - len(content.lstrip())]
        indent = f"{indent}{extra}"
        content = content.lstrip()
        stripped = content.rstrip()
        suffix = "," if stripped.endswith(",") else ""
        if suffix:
            stripped = stripped[:-1].rstrip()
        if len(stripped) < 2 or stripped[0] not in ("'", '"') or stripped[-1] != stripped[0]:
            return None
        quote = stripped[0]
        value = stripped[1:-1]
        chunk_width = max(1, width - len(indent) - 8)
        chunks = textwrap.wrap(value, width=chunk_width, break_long_words=False, break_on_hyphens=False)
        if len(chunks) <= 1:
            return None
        return "\n".join(
            [
                f"{indent}(",
                *(f"{indent}    {quote}{chunk}{quote}" for chunk in chunks),
                f"{indent}){suffix}",
            ]
        )

    def _render_expression(self, expression: str, *, indent: str, width: int) -> str:
        lines: list[str] = []
        current = indent
        depth = 0
        quote = ""
        escaped = False
        index = 0
        while index < len(expression):
            character = expression[index]
            if quote:
                current += character
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == quote:
                    quote = ""
                index += 1
                continue

            if character in ("'", '"'):
                quote = character
                current += character
            elif character in "([{":
                current += character
                lines.append(current)
                depth += 1
                current = f"{indent}{'    ' * depth}"
            elif character in ")]}":
                if current.strip():
                    lines.append(current)
                depth = max(0, depth - 1)
                current = f"{indent}{'    ' * depth}{character}"
            elif character == "," and depth:
                current += character
                lines.append(current)
                current = f"{indent}{'    ' * depth}"
            elif character == "." and depth == 0 and len(current) > len(indent):
                lines.append(current)
                current = f"{indent}."
            else:
                current += character
            index += 1
        if current.strip():
            lines.append(current)
        return "\n".join(lines)

    def _split(self, content: str, marker: str) -> tuple[str, str]:
        if marker == "return ":
            if content.startswith(marker):
                return marker, content[len(marker) :]
            return "", ""
        if marker in content:
            prefix, expression = content.split(marker, 1)
            return f"{prefix}{marker}", expression
        return "", ""

    def _indent(self, line: str) -> str:
        return line[: len(line) - len(line.lstrip())]
