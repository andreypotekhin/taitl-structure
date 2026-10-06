"""Optional Delta Lake declarations; vendor imports occur only during execution."""

from structure.plugin.pyspark.delta.checks import BoundCheck, Check, bind_checks, check
from structure.plugin.pyspark.delta.bindings import delta_input, delta_output

__all__ = [
    "BoundCheck",
    "Check",
    "bind_checks",
    "check",
    "delta_input",
    "delta_output",
    "delta_delete",
    "delta_update",
    "delta_merge",
    "delta_append",
    "delta_replace_where",
    "delta_snapshot",
    "delta_changes",
]


def __getattr__(name: str):
    if name in {"delta_delete", "delta_update", "delta_merge", "delta_append", "delta_replace_where", "delta_snapshot", "delta_changes"}:
        from structure.plugin.pyspark.delta import operations

        return getattr(operations, name)
    raise AttributeError(name)
