"""Optional Delta Lake declarations; vendor imports occur only during execution."""

from structure.plugin.pyspark.delta.checks import BoundCheck, Check, bind_checks, check
from structure.plugin.pyspark.delta.bindings import delta_input, delta_output, delta_table
from structure.plugin.pyspark.delta.schema import delta_default, delta_generated, delta_identity

__all__ = [
    "BoundCheck",
    "Check",
    "bind_checks",
    "check",
    "delta_input",
    "delta_output",
    "delta_table",
    "delta_generated",
    "delta_identity",
    "delta_default",
    "delta_delete",
    "delta_update",
    "delta_merge",
    "delta_append",
    "delta_replace_where",
    "delta_snapshot",
    "delta_changes",
    "delta_history",
    "delta_detail",
    "delta_restore",
    "delta_optimize",
    "delta_vacuum",
]


def __getattr__(name: str):
    if name in {"delta_delete", "delta_update", "delta_merge", "delta_append", "delta_replace_where", "delta_snapshot", "delta_changes", "delta_history", "delta_detail", "delta_restore", "delta_optimize", "delta_vacuum"}:
        from structure.plugin.pyspark.delta import operations

        return getattr(operations, name)
    raise AttributeError(name)
