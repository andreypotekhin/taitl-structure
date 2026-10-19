"""Internal wrapper for PySpark ``@special`` function behavior."""

from __future__ import annotations

import inspect
from datetime import date, datetime
from inspect import signature
from typing import Any, Callable, cast, get_type_hints

from structure.plugin.api.v1.model import current_symbolic_context
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructureType,
    TimestampType,
)


class SpecialFunction:
    """Dispatch a decorated Python helper according to its special mode.

    ``expr`` helpers expand into symbolic expression trees and ``udf`` helpers
    become Python UDF expression nodes inside compilation. ``ignore`` and
    ``opaque`` are rejected by the target-neutral wrapper before reaching this class.
    """

    def __init__(
        self,
        function: Callable,
        *,
        type: str,
        return_type: object | None = None,
        nullable: bool = True,
        function_type: str = "scalar",
        deterministic: bool | None = None,
    ) -> None:
        self.function = function
        self.type = type
        self.return_type = return_type
        self.nullable = nullable
        self.function_type = function_type
        self.deterministic = deterministic
        self.__name__ = function.__name__
        self.__qualname__ = function.__qualname__
        self.__module__ = function.__module__

    def __call__(self, *args, **kwargs):
        """Evaluate normally outside compilation or return a symbolic special expression."""
        context = current_symbolic_context()
        if self.type == "expr":
            return self._expr(args, kwargs, context=context)
        if self.type == "udf":
            if context is None:
                return self.function(*args, **kwargs)
            if kwargs:
                raise TypeError("@special(type=\"udf\") calls only support positional expression arguments")
            return self._udf(args)
        if self.type == "arrow_udf":
            if context is None:
                return self.function(*args, **kwargs)
            return self._arrow_udf(args, kwargs)
        return self.function(*args, **kwargs)

    def _expr(self, args: tuple[object, ...], kwargs: dict[str, object], *, context) -> Expression:
        expanded = literal(self.function(*args, **kwargs))
        if context is None or not context.capture_special_exprs:
            return expanded

        bound = signature(self.function).bind(*args, **kwargs)
        bound.apply_defaults()
        arguments = {name: literal(value) for name, value in bound.arguments.items()}
        generic_arguments = {
            name: Expression(kind="lambda_arg", type=value.type, nullable=value.nullable, data={"name": name})
            for name, value in arguments.items()
        }
        body = literal(self.function(**generic_arguments))
        return Expression(
            kind="special_expr",
            type=expanded.type,
            nullable=expanded.nullable,
            data={
                "name": self.function.__name__,
                "parameters": tuple(arguments),
                "body": body,
                "expanded": expanded,
                "keyword_arguments": tuple((name, literal(value)) for name, value in kwargs.items()),
            },
            args=tuple(literal(argument) for argument in args),
        )

    def __get__(self, instance: object, owner: type | None = None):
        return self if instance is None else self.__call__

    def _udf(self, args: tuple[object, ...]) -> Expression:
        return_type = self._return_type()
        return Expression(
            kind="python_udf",
            type=return_type if isinstance(return_type, StructureType) else None,
            nullable=self.nullable,
            data={
                "function": self.function,
                "function_name": self.function.__name__,
                "module": self.function.__module__,
                "qualname": self.function.__qualname__,
                "udf_name": self._udf_name(),
                "return_type": return_type,
                "pyspark_return_type": self._is_pyspark_type(return_type),
            },
            args=tuple(literal(arg) for arg in args),
        )

    def _arrow_udf(self, args: tuple[object, ...], kwargs: dict[str, object]) -> Expression:
        from structure.plugin.pyspark.dsl.operations_api import _aggregate
        from structure.plugin.pyspark.dsl.windows.WindowSpec import WindowSpec

        over = kwargs.pop("over", None)
        if over is not None and (self.function_type != "grouped_agg" or not isinstance(over, WindowSpec)):
            raise TypeError('Only grouped_agg Arrow UDF calls accept over=WindowSpec')
        if self.function_type == "scalar_iter":
            self._bind_iterator(args)
        else:
            signature(self.function).bind(*args, **kwargs)
        if self.function_type == "scalar" and not any(
            parameter.kind in {
                parameter.POSITIONAL_ONLY,
                parameter.POSITIONAL_OR_KEYWORD,
                parameter.VAR_POSITIONAL,
            }
            for parameter in signature(self.function).parameters.values()
        ):
            raise TypeError("Spark scalar Arrow UDFs require at least one positional callback parameter")

        return_type = self._arrow_return_type()
        keyword_names = tuple(kwargs)
        arguments = tuple(literal(value) for value in (*args, *kwargs.values()))
        data = {
            "function": self.function,
            "function_name": self.function.__name__,
            "module": self.function.__module__,
            "qualname": self.function.__qualname__,
            "udf_name": self._udf_name(),
            "return_type": return_type,
            "function_type": self.function_type,
            "deterministic": self.deterministic,
            "positional_count": len(args),
            "keyword_names": keyword_names,
        }
        if self.function_type in {"scalar", "scalar_iter"}:
            data["capability_group"] = "expression"
            data["capability_name"] = f"arrow_udf_{self.function_type}"
            return Expression(kind="arrow_udf", type=return_type, nullable=self.nullable, data=data, args=arguments)
        if over is None:
            aggregate_options = tuple(
                (f"arrow_{key}", value)
                for key, value in data.items()
                if key not in {"capability_group", "capability_name"}
            )
            return _aggregate(
                "arrow_udf_grouped_agg",
                *arguments,
                type=return_type,
                nullable=self.nullable,
                options=aggregate_options,
            )
        data.update(
            {
                "capability_group": "window",
                "capability_name": "arrow_udf_grouped_agg",
                "order_count": len(over.order_by),
                "partition_count": len(over.partition_by),
            }
        )
        if over.frame is not None:
            data.update(
                {
                    "frame_kind": over.frame.kind,
                    "frame_start": over.frame.start.as_pyspark(),
                    "frame_end": over.frame.end.as_pyspark(),
                }
            )
        return Expression(
            kind="arrow_udf_window",
            type=return_type,
            nullable=self.nullable,
            data=data,
            args=(*arguments, *over.order_by, *over.partition_by),
        )

    def _bind_iterator(self, args: tuple[object, ...]) -> None:
        if not args:
            raise TypeError("scalar_iter Arrow UDF calls require at least one positional column expression")
        signature(self.function).bind(object())

    def _arrow_return_type(self) -> StructureType:
        value = self.return_type
        if isinstance(value, StructureType):
            return value
        if isinstance(value, type) and issubclass(value, StructureType):
            return cast(Any, value)()
        raise TypeError(f'{self.function.__qualname__} @special(type="arrow_udf") needs a Structure return_type')

    def _return_type(self) -> object:
        if self.return_type is not None:
            return self._normalize_type(self.return_type)
        annotation = get_type_hints(self.function).get("return")
        if annotation is not None:
            return self._annotation_type(annotation)
        raise TypeError(
            f"{self.function.__qualname__} @special(type=\"udf\") needs return_type=... or a supported return annotation"
        )

    def _normalize_type(self, value: object) -> object:
        if isinstance(value, StructureType) or self._is_pyspark_type(value):
            return value
        if isinstance(value, type) and issubclass(value, StructureType):
            return cast(Any, value)()
        return self._annotation_type(value)

    def _annotation_type(self, value: object) -> StructureType:
        if value is str:
            return StringType()
        if value is int:
            return IntegerType()
        if value is float:
            return DoubleType()
        if value is bool:
            return BooleanType()
        if value is date:
            return DateType()
        if value is datetime:
            return TimestampType()
        raise TypeError(f"{self.function.__qualname__} has unsupported UDF return type {value!r}")

    @staticmethod
    def _is_pyspark_type(value: object) -> bool:
        return type(value).__module__.startswith("pyspark.sql.types") and type(value).__name__.endswith("Type")

    def _udf_name(self) -> str:
        parts = [character.lower() if character.isalnum() else "_" for character in f"{self.function.__module__}_{self.function.__qualname__}"]
        return f"_structure_udf_{''.join(parts).strip('_')}"
