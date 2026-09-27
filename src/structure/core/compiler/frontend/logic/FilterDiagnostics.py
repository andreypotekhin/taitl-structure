from __future__ import annotations

from collections.abc import Iterable

from structure.core.configuration.model.ConfigDiagnostic import ConfigDiagnostic
from structure.core.configuration.model.ConfigError import ConfigError
from structure.lib.cross.errors import diagnostic_registry


class FilterDiagnostics:
    """Apply project and transform warning policy after all compiler phases run."""

    _LINEAGE_WARNINGS = frozenset(
        {
            "PYSPARK-W2701",
            "PYSPARK-W2702",
            "PYSPARK-W2703",
            "PYSPARK-W2704",
        }
    )

    def __call__(
        self,
        diagnostics: Iterable[object],
        *,
        disable: Iterable[str] = (),
        warn_on_udfs: bool = True,
        warn_on_lineage_growth: bool = True,
    ) -> tuple[object, ...]:
        disabled = self._validate(disable)
        if not warn_on_udfs:
            disabled.add("DSL-W0403")
        if not warn_on_lineage_growth:
            disabled.update(self._LINEAGE_WARNINGS)
        return tuple(diagnostic for diagnostic in diagnostics if getattr(diagnostic, "code", None) not in disabled)

    @staticmethod
    def _validate(codes: Iterable[str]) -> set[str]:
        values = tuple(codes)
        if len(values) != len(set(values)):
            raise ConfigError(
                ConfigDiagnostic(
                    code="CONF-E0102",
                    setting="disable",
                    problem="disable must not contain duplicate warning codes",
                    use='Use each warning code once, for example disable = ["PYSPARK-W2701"].',
                )
            )
        entries = {entry.code: entry for entry in diagnostic_registry.entries()}
        for code in values:
            entry = entries.get(code)
            if entry is None:
                raise ConfigError(
                    ConfigDiagnostic(
                        code="CONF-E0102",
                        setting="disable",
                        problem=f"Unknown diagnostic code: {code}",
                        use="Use an active warning code listed in docs/Diagnostics.md.",
                    )
                )
            if entry.severity != "warning":
                raise ConfigError(
                    ConfigDiagnostic(
                        code="CONF-E0102",
                        setting="disable",
                        problem=f"Only warning diagnostics can be disabled: {code}",
                        use="Remove error, info, or internal diagnostic codes from disable.",
                    )
                )
        return set(values)
