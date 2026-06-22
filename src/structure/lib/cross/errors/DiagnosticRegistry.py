from __future__ import annotations

import re
from collections.abc import Iterable

from structure.lib.cross.errors.DiagnosticEntry import DiagnosticEntry


class DiagnosticRegistry:

    _code = re.compile(r"^(?P<prefix>[A-Z]+)-(?P<letter>[EWIX])(?P<number>[0-9]{4})$")
    _prefixes = {
        "BACKEND",
        "CLI",
        "CONF",
        "CORE",
        "DISC",
        "DSL",
        "GEN",
        "HOOK",
        "IR",
        "JOIN",
        "ONLINE",
        "SCHEMA",
        "STREAM",
        "VAL",
    }
    _severities = {
        "error": "E",
        "warning": "W",
        "info": "I",
        "internal": "X",
    }
    _statuses = {"draft", "active", "deprecated", "retired"}

    def __init__(self, entries: Iterable[DiagnosticEntry]) -> None:
        self._entries = {entry.code: entry for entry in entries}
        self._duplicates = self._duplicate_codes(entries)
        self.validate()

    def __getitem__(self, code: str) -> DiagnosticEntry:
        return self._entries[code]

    def get(self, code: str) -> DiagnosticEntry:
        return self._entries[code]

    def entries(self) -> tuple[DiagnosticEntry, ...]:
        return tuple(self._entries[code] for code in sorted(self._entries))

    def validate(self) -> None:
        if self._duplicates:
            raise ValueError(f"Duplicate diagnostic code(s): {', '.join(sorted(self._duplicates))}")
        for entry in self._entries.values():
            self._validate(entry)

    def _validate(self, entry: DiagnosticEntry) -> None:
        match = self._code.match(entry.code)
        if match is None:
            raise ValueError(f"Malformed diagnostic code: {entry.code}")
        if match.group("prefix") not in self._prefixes:
            raise ValueError(f"Unknown diagnostic prefix: {entry.code}")
        if entry.severity not in self._severities:
            raise ValueError(f"Unknown diagnostic severity: {entry.code}")
        if match.group("letter") != self._severities[entry.severity]:
            raise ValueError(f"Diagnostic severity does not match code letter: {entry.code}")
        if entry.status not in self._statuses:
            raise ValueError(f"Unknown diagnostic status: {entry.code}")
        if entry.status in {"active", "deprecated", "retired"} and not entry.docs:
            raise ValueError(f"Published diagnostic is missing docs link: {entry.code}")
        if entry.status == "deprecated" and not entry.replaced_by:
            raise ValueError(f"Deprecated diagnostic is missing replacement: {entry.code}")

    def _duplicate_codes(self, entries: Iterable[DiagnosticEntry]) -> set[str]:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for entry in entries:
            if entry.code in seen:
                duplicates.add(entry.code)
            seen.add(entry.code)
        return duplicates
