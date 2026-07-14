from __future__ import annotations

from datetime import date, datetime
from inspect import signature
from typing import Any, Callable, cast, get_type_hints

from structure.app.compiler.symbolic_execution.model.CompileContext import current_context
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.types.BooleanType import BooleanType
from structure.app.dsl.model.types.DateType import DateType
from structure.app.dsl.model.types.DoubleType import DoubleType
from structure.app.dsl.model.types.IntegerType import IntegerType
from structure.app.dsl.model.types.StringType import StringType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.app.dsl.model.types.TimestampType import TimestampType


class SpecialFunction:

    def __init__(
        self,
        function: Callable,
        *,
        type: str,
        return_type: object | None = None,
        nullable: bool = True,
    ) -> None:
        self.function = function
        self.type = type
        self.return_type = return_type
        self.nullable = nullable
        self.__name__ = function.__name__
        self.__qualname__ = function.__qualname__
        self.__module__ = function.__module__

    def __call__(self, *args, **kwargs):
        if self.type == "expr":
            return self._expr(args, kwargs)
        if self.type == "udf":
            if current_context() is None:
                return self.function(*args, **kwargs)
            if kwargs:
                raise TypeError("@special(type=\"udf\") calls only support positional expression arguments")
            return self._udf(args)
        if self.type == "opaque" and current_context() is not None:
            raise TypeError(
                f"{self.function.__qualname__} is @special(type=\"opaque\") and cannot be called from a compiled "
                "step method. Use @special(type=\"udf\") for scalar Python UDFs or a hook for DataFrame logic."
            )
        return self.function(*args, **kwargs)

    def _expr(self, args: tuple[object, ...], kwargs: dict[str, object]) -> Expression:
        expanded = literal(self.function(*args, **kwargs))
        context = current_context()
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
        if instance is None:
            return self
        return self.__call__

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

    def _is_pyspark_type(self, value: object) -> bool:
        module = type(value).__module__
        return module.startswith("pyspark.sql.types") and type(value).__name__.endswith("Type")

    def _udf_name(self) -> str:
        parts = [
            character.lower() if character.isalnum() else "_"
            for character in f"{self.function.__module__}_{self.function.__qualname__}"
        ]
        return f"_structure_udf_{''.join(parts).strip('_')}"
