from __future__ import annotations

from structure.dsl import Schema
from structure.plugin.api.v1.model import current_symbolic_context
from structure.plugin.pyspark.dsl.logic import CapturePySparkGenerator
from structure.plugin.pyspark.dsl.RowScope import RowScope

_generators = CapturePySparkGenerator()


def explode_struct(
    value: object,
    *,
    as_: type[Schema],
    scope: str | None = None,
) -> RowScope:
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("explode_struct(...) can only be used inside a compiled Structure step method")
    return _generators.explode_struct(context, value, as_=as_, scope=scope)


def posexplode_struct(
    value: object,
    *,
    as_: type[Schema],
    ordinal: str = "ordinal",
    scope: str | None = None,
) -> RowScope:
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("posexplode_struct(...) can only be used inside a compiled Structure step method")
    return _generators.posexplode_struct(context, value, as_=as_, ordinal=ordinal, scope=scope)
