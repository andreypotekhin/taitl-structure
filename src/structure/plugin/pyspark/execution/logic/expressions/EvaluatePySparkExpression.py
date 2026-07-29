from typing import Any, cast

from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.dsl.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructType,
    StructureType,
    TimestampType,
)


class EvaluatePySparkExpression:

    @property
    def _schema(self):
        from structure.plugin.pyspark.api.PySpark import PySpark

        return PySpark.schema

    def evaluate(self, expression: PySparkExpressionRecipe, *, functions, aliases, window=None):
        if expression.kind == "field":
            if "column" in expression.data:
                return expression.data["column"]
            scope = str(expression.data["scope"])
            alias = aliases.get(scope, scope)
            if alias == "":
                return functions.col(self._field_path(expression))
            return functions.col(f"{alias}.{self._field_path(expression)}")
        if expression.kind == "get_field":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window).getField(
                expression.data["field"]
            )
        if expression.kind == "with_field":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window).withField(
                expression.data["field"],
                self.evaluate(expression.args[1], functions=functions, aliases=aliases, window=window),
            )
        if expression.kind == "drop_fields":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window).dropFields(
                *cast(tuple[str, ...], expression.data["fields"])
            )
        if expression.kind == "struct":
            return self._struct(expression, functions=functions, aliases=aliases, window=window)
        if expression.kind == "literal":
            return functions.lit(expression.data["value"])
        if expression.kind == "lambda_arg":
            return expression.data["column"]
        if expression.kind == "call":
            return self._call(expression, functions=functions, aliases=aliases, window=window)
        if expression.kind == "python_udf":
            return self._python_udf(expression, functions=functions, aliases=aliases, window=window)
        if expression.kind == "time_window":
            arguments = [
                self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window),
                expression.data["duration"],
            ]
            if expression.data.get("slide") is not None:
                arguments.append(expression.data["slide"])
            if expression.data.get("start") is not None:
                arguments.append(expression.data["start"])
            return functions.window(*arguments)
        if expression.kind == "transform_expression":
            return self._reserved(expression, functions=functions, aliases=aliases, window=window)
        if expression.kind == "is_not_null":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window).isNotNull()
        if expression.kind == "is_null":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window).isNull()
        if expression.kind == "is_nan":
            return functions.isnan(
                self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window)
            )
        if expression.kind == "and":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="and")
        if expression.kind == "or":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="or")
        if expression.kind == "eq":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="eq")
        if expression.kind == "ne":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="ne")
        if expression.kind == "gt":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="gt")
        if expression.kind == "lt":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="lt")
        if expression.kind == "le":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="le")
        if expression.kind == "ge":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="ge")
        if expression.kind == "add":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="add")
        if expression.kind == "sub":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="sub")
        if expression.kind == "mul":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="mul")
        if expression.kind == "div":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="div")
        if expression.kind == "mod":
            return self._binary(expression, functions=functions, aliases=aliases, window=window, operator="mod")
        if expression.kind == "neg":
            return -self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window)
        if expression.kind in {"bitwise_and", "bitwise_or", "bitwise_xor"}:
            return self._bitwise_binary(expression, functions=functions, aliases=aliases, window=window)
        if expression.kind == "bitwise_not":
            return functions.bitwise_not(
                self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window)
            )
        if expression.kind == "when":
            condition, value, fallback = (
                self.evaluate(argument, functions=functions, aliases=aliases, window=window)
                for argument in expression.args
            )
            return functions.when(condition, value).otherwise(fallback)
        if expression.kind == "event_time_between":
            left, right = (
                self.evaluate(argument, functions=functions, aliases=aliases, window=window)
                for argument in expression.args
            )
            lower = functions.expr(f"INTERVAL {expression.data['lower']}")
            upper = functions.expr(f"INTERVAL {expression.data['upper']}")
            return (right >= left - lower) & (right <= left + upper)
        if expression.kind == "null_safe_eq":
            left, right = expression.args
            return self.evaluate(left, functions=functions, aliases=aliases, window=window).eqNullSafe(
                self.evaluate(right, functions=functions, aliases=aliases, window=window)
            )
        if expression.kind == "isin":
            value, *items = expression.args
            return self.evaluate(value, functions=functions, aliases=aliases, window=window).isin(
                *(self.evaluate(item, functions=functions, aliases=aliases, window=window) for item in items)
            )
        if expression.kind in {"contains", "startswith", "endswith", "like", "ilike", "rlike"}:
            value = self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window)
            return getattr(value, expression.kind)(expression.data["pattern"])
        if expression.kind == "item":
            collection, key = expression.args
            item = (
                key.data["value"]
                if key.kind == "literal"
                else self.evaluate(key, functions=functions, aliases=aliases, window=window)
            )
            return self.evaluate(collection, functions=functions, aliases=aliases, window=window)[item]
        if expression.kind == "cast":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window).cast(
                expression.data["spark_type"]
            )
        if expression.kind == "try_cast":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window).try_cast(
                expression.data["spark_type"]
            )
        if expression.kind == "order":
            return getattr(
                self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window),
                str(expression.data["direction"]),
            )()
        if expression.kind == "not":
            return ~self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window)
        raise TypeError(f"Unsupported PySpark expression recipe: {expression.kind}")

    def _struct(self, expression: PySparkExpressionRecipe, *, functions, aliases, window=None):
        fields = cast(tuple[Any, ...], expression.data["fields"])
        columns = (
            self.evaluate(argument, functions=functions, aliases=aliases, window=window).alias(field.column)
            for field, argument in zip(fields, expression.args, strict=True)
        )
        return functions.struct(*columns)

    def _field_path(self, expression: PySparkExpressionRecipe) -> str:
        path = expression.data.get("path")
        if not isinstance(path, tuple):
            return str(expression.data["field"])
        return ".".join(self._field_segment(str(segment)) for segment in path)

    def _field_segment(self, value: str) -> str:
        if "." not in value and "`" not in value:
            return value
        return f"`{value.replace('`', '``')}`"

    def _reserved(self, expression: PySparkExpressionRecipe, *, functions, aliases, window):
        function = expression.data["function"]
        if function == "session_window":
            return functions.session_window(
                self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window),
                expression.data["gap"],
            )
        if function == "array_transform":
            array, body = expression.args
            name = self._lambda_name(expression)
            return functions.transform(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                lambda item: self.evaluate(
                    self._bind_lambdas(body, {name: item}), functions=functions, aliases=aliases, window=window
                ),
            )
        if function == "array_filter":
            array, body = expression.args
            name = self._lambda_name(expression)
            return functions.filter(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                lambda item: self.evaluate(
                    self._bind_lambdas(body, {name: item}), functions=functions, aliases=aliases, window=window
                ),
            )
        if function == "array_exists":
            array, body = expression.args
            name = self._lambda_name(expression)
            return functions.exists(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                lambda item: self.evaluate(
                    self._bind_lambdas(body, {name: item}), functions=functions, aliases=aliases, window=window
                ),
            )
        if function == "array_forall":
            array, body = expression.args
            name = self._lambda_name(expression)
            return functions.forall(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                lambda item: self.evaluate(
                    self._bind_lambdas(body, {name: item}), functions=functions, aliases=aliases, window=window
                ),
            )
        if function == "array_zip_with":
            left, right, _, _, body = expression.args
            return functions.zip_with(
                self.evaluate(left, functions=functions, aliases=aliases, window=window),
                self.evaluate(right, functions=functions, aliases=aliases, window=window),
                lambda left_item, right_item: self.evaluate(
                    self._bind_lambdas(body, {"left_item": left_item, "right_item": right_item}),
                    functions=functions,
                    aliases=aliases,
                    window=window,
                ),
            )
        if function == "array_aggregate":
            array, initial, _, _, merged, finished = expression.args
            aggregate_arguments = (
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                self.evaluate(initial, functions=functions, aliases=aliases, window=window),
                lambda acc, item: self.evaluate(
                    self._bind_lambdas(merged, {"acc": acc, "item": item}),
                    functions=functions,
                    aliases=aliases,
                    window=window,
                ),
            )
            if finished == merged:
                return functions.aggregate(*aggregate_arguments)
            return functions.aggregate(
                *aggregate_arguments,
                lambda acc: self.evaluate(
                    self._bind_lambdas(finished, {"acc": acc}),
                    functions=functions,
                    aliases=aliases,
                    window=window,
                ),
            )
        if function == "array_sort_by":
            array, left_key, right_key = expression.args
            return functions.array_sort(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                lambda left, right: self._array_sort_comparator(
                    left_key,
                    right_key,
                    left=left,
                    right=right,
                    descending=bool(expression.data.get("descending")),
                    functions=functions,
                    aliases=aliases,
                    window=window,
                ),
            )
        if function == "array_sort":
            [array] = expression.args
            return functions.array_sort(self.evaluate(array, functions=functions, aliases=aliases, window=window))
        if function == "array_reverse":
            [array] = expression.args
            return functions.reverse(self.evaluate(array, functions=functions, aliases=aliases, window=window))
        if function == "array_flatten":
            [array] = expression.args
            return functions.flatten(self.evaluate(array, functions=functions, aliases=aliases, window=window))
        if function == "array_distinct":
            [array] = expression.args
            return functions.array_distinct(self.evaluate(array, functions=functions, aliases=aliases, window=window))
        if function == "array_position":
            array, item = expression.args
            needle = (
                item.data["value"]
                if item.kind == "literal"
                else self.evaluate(
                    item,
                    functions=functions,
                    aliases=aliases,
                    window=window,
                )
            )
            return functions.array_position(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                needle,
            )
        if function == "collection_size":
            [value] = expression.args
            return functions.size(self.evaluate(value, functions=functions, aliases=aliases, window=window))
        if function in {"array_contains", "map_contains_key"}:
            collection, item = expression.args
            needle = (
                item.data["value"]
                if item.kind == "literal"
                else self.evaluate(item, functions=functions, aliases=aliases, window=window)
            )
            return getattr(functions, function)(
                self.evaluate(collection, functions=functions, aliases=aliases, window=window), needle
            )
        if function == "array":
            return functions.array(
                *(
                    self.evaluate(value, functions=functions, aliases=aliases, window=window)
                    for value in expression.args
                )
            )
        if function == "array_repeat":
            value, repeat_count = expression.args
            repeats = (
                repeat_count.data["value"]
                if repeat_count.kind == "literal"
                else self.evaluate(repeat_count, functions=functions, aliases=aliases, window=window)
            )
            return functions.array_repeat(
                self.evaluate(value, functions=functions, aliases=aliases, window=window), repeats
            )
        if function == "array_sequence":
            return functions.sequence(
                *(
                    self.evaluate(value, functions=functions, aliases=aliases, window=window)
                    for value in expression.args
                )
            )
        if function in {"array_append", "array_prepend"}:
            array, item = expression.args
            return getattr(functions, function)(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                self.evaluate(item, functions=functions, aliases=aliases, window=window),
            )
        if function == "array_insert":
            array, position, item = expression.args
            return functions.array_insert(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                position.data["value"],
                self.evaluate(item, functions=functions, aliases=aliases, window=window),
            )
        if function == "array_remove":
            array, item = expression.args
            return functions.array_remove(
                self.evaluate(array, functions=functions, aliases=aliases, window=window), item.data["value"]
            )
        if function == "array_compact":
            [array] = expression.args
            return functions.array_compact(self.evaluate(array, functions=functions, aliases=aliases, window=window))
        if function == "array_slice":
            value, start, length = expression.args
            offset = (
                start.data["value"]
                if start.kind == "literal"
                else self.evaluate(start, functions=functions, aliases=aliases, window=window)
            )
            size = (
                length.data["value"]
                if length.kind == "literal"
                else self.evaluate(length, functions=functions, aliases=aliases, window=window)
            )
            return functions.slice(
                self.evaluate(value, functions=functions, aliases=aliases, window=window), offset, size
            )
        if function in {"array_union", "array_except", "array_intersect"}:
            left, right = expression.args
            return getattr(functions, function)(
                self.evaluate(left, functions=functions, aliases=aliases, window=window),
                self.evaluate(right, functions=functions, aliases=aliases, window=window),
            )
        if function in {"element_at", "try_element_at"}:
            collection, key = expression.args
            return getattr(functions, function)(
                self.evaluate(collection, functions=functions, aliases=aliases, window=window),
                self.evaluate(key, functions=functions, aliases=aliases, window=window),
            )
        if function == "map_concat":
            return functions.map_concat(
                *(
                    self.evaluate(value, functions=functions, aliases=aliases, window=window)
                    for value in expression.args
                )
            )
        if function == "map_transform_values":
            mapping, _, _, body = expression.args
            return functions.transform_values(
                self.evaluate(mapping, functions=functions, aliases=aliases, window=window),
                lambda key, value: self.evaluate(
                    self._bind_lambdas(body, {"key": key, "value": value}),
                    functions=functions,
                    aliases=aliases,
                    window=window,
                ),
            )
        if function == "map_filter":
            mapping, _, _, body = expression.args
            return functions.map_filter(
                self.evaluate(mapping, functions=functions, aliases=aliases, window=window),
                lambda key, value: self.evaluate(
                    self._bind_lambdas(body, {"key": key, "value": value}),
                    functions=functions,
                    aliases=aliases,
                    window=window,
                ),
            )
        if function == "map_transform_keys":
            mapping, _, _, body = expression.args
            return functions.transform_keys(
                self.evaluate(mapping, functions=functions, aliases=aliases, window=window),
                lambda key, value: self.evaluate(
                    self._bind_lambdas(body, {"key": key, "value": value}),
                    functions=functions,
                    aliases=aliases,
                    window=window,
                ),
            )
        if function == "map_zip_with":
            left, right, _, _, _, body = expression.args
            return functions.map_zip_with(
                self.evaluate(left, functions=functions, aliases=aliases, window=window),
                self.evaluate(right, functions=functions, aliases=aliases, window=window),
                lambda key, left_value, right_value: self.evaluate(
                    self._bind_lambdas(
                        body,
                        {"key": key, "left_value": left_value, "right_value": right_value},
                    ),
                    functions=functions,
                    aliases=aliases,
                    window=window,
                ),
            )
        if function in {"map_keys", "map_values", "map_entries", "map_from_entries"}:
            [value] = expression.args
            name = {
                "map_keys": "map_keys",
                "map_values": "map_values",
                "map_entries": "map_entries",
                "map_from_entries": "map_from_entries",
            }[function]
            return getattr(functions, name)(self.evaluate(value, functions=functions, aliases=aliases, window=window))
        if function in {"window_row_number", "window_rank", "window_dense_rank"}:
            order_by, partition_by = self._window_arguments(expression, 0)
            # Spark Connect materializes these functions as int, unlike the LongType
            # promised by Structure's expression contract and emitted generated code.
            return (
                getattr(functions, function.removeprefix("window_"))()
                .over(
                    self._window(
                        order_by,
                        partition_by,
                        expression,
                        functions=functions,
                        aliases=aliases,
                        window=window,
                        include_frame=False,
                    )
                )
                .cast("long")
            )
        if function in {"window_percent_rank", "window_cume_dist"}:
            order_by, partition_by = self._window_arguments(expression, 0)
            return getattr(functions, function.removeprefix("window_"))().over(
                self._window(
                    order_by,
                    partition_by,
                    expression,
                    functions=functions,
                    aliases=aliases,
                    window=window,
                    include_frame=False,
                )
            )
        if function == "window_ntile":
            order_by, partition_by = self._window_arguments(expression, 0)
            return functions.ntile(expression.data["buckets"]).over(
                self._window(
                    order_by,
                    partition_by,
                    expression,
                    functions=functions,
                    aliases=aliases,
                    window=window,
                    include_frame=False,
                )
            )
        if function in {"window_lag", "window_lead"}:
            [value] = expression.args[:1]
            order_by, partition_by = self._window_arguments(expression, 1)
            arguments = [
                self.evaluate(value, functions=functions, aliases=aliases, window=window),
                expression.data["offset"],
            ]
            if expression.data.get("has_default"):
                arguments.append(expression.data["default"])
            return getattr(functions, function.removeprefix("window_"))(*arguments).over(
                self._window(order_by, partition_by, expression, functions=functions, aliases=aliases, window=window)
            )
        if function in {"window_rolling_sum", "window_rolling_avg", "window_rolling_min", "window_rolling_max"}:
            [value] = expression.args[:1]
            order_by, partition_by = self._window_arguments(expression, 1)
            column = self.evaluate(value, functions=functions, aliases=aliases, window=window)
            return getattr(functions, function.removeprefix("window_rolling_"))(column).over(
                self._window(order_by, partition_by, expression, functions=functions, aliases=aliases, window=window)
            )
        if function in {"window_first_value", "window_last_value", "window_nth_value"}:
            count = self._int_data(expression, "value_count", 1)
            values = expression.args[:count]
            order_by, partition_by = self._window_arguments(expression, count)
            arguments = [self.evaluate(values[0], functions=functions, aliases=aliases, window=window)]
            if function == "window_nth_value":
                arguments.append(expression.data["n"])
            if expression.data.get("ignore_nulls"):
                arguments.append(True)
            return getattr(functions, function.removeprefix("window_"))(*arguments).over(
                self._window(order_by, partition_by, expression, functions=functions, aliases=aliases, window=window)
            )
        if function in {
            "window_sum",
            "window_avg",
            "window_min",
            "window_max",
            "window_count",
            "window_bool_and",
            "window_bool_or",
            "window_stddev",
            "window_variance",
            "window_collect_list",
            "window_collect_set",
        }:
            count = self._int_data(expression, "value_count", 1)
            values = expression.args[:count]
            order_by, partition_by = self._window_arguments(expression, count)
            argument = (
                self.evaluate(values[0], functions=functions, aliases=aliases, window=window)
                if values
                else functions.lit(1)
            )
            return getattr(functions, function.removeprefix("window_"))(argument).over(
                self._window(order_by, partition_by, expression, functions=functions, aliases=aliases, window=window)
            )
        raise TypeError(f"Unsupported PySpark reserved expression: {function}")

    def _window(self, order_by, partition_by, expression, *, functions, aliases, window, include_frame=True):
        if window is None:
            raise TypeError("Window expression evaluation requires a PySpark Window module")
        partitions = [
            self.evaluate(partition, functions=functions, aliases=aliases, window=window) for partition in partition_by
        ]
        ordering = [
            self._window_order(order, expression, functions=functions, aliases=aliases, window=window)
            for order in order_by
        ]
        spec = window.partitionBy(*partitions).orderBy(*ordering)
        if not include_frame:
            return spec
        if "frame_kind" in expression.data:
            frame = spec.rowsBetween if expression.data["frame_kind"] == "rows" else spec.rangeBetween
            return frame(
                self._window_bound(expression.data["frame_start"], window),
                self._window_bound(expression.data["frame_end"], window),
            )
        if "preceding" in expression.data:
            return spec.rowsBetween(-int(expression.data["preceding"]), 0)
        return spec

    def _array_sort_comparator(
        self,
        left_key,
        right_key,
        *,
        left,
        right,
        descending,
        functions,
        aliases,
        window,
    ):
        left_value = self.evaluate(
            self._bind_lambdas(left_key, {"left": left}), functions=functions, aliases=aliases, window=window
        )
        right_value = self.evaluate(
            self._bind_lambdas(right_key, {"right": right}), functions=functions, aliases=aliases, window=window
        )
        null_order = 1 if descending else -1
        value_order = 1 if descending else -1
        return (
            functions.when(left_value.isNull() & right_value.isNotNull(), functions.lit(null_order))
            .when(left_value.isNotNull() & right_value.isNull(), functions.lit(-null_order))
            .when(left_value < right_value, functions.lit(value_order))
            .when(left_value > right_value, functions.lit(-value_order))
            .otherwise(functions.lit(0))
        )

    def _window_arguments(self, expression, value_count):
        order_count = self._int_data(expression, "order_count", 1)
        orders = list(expression.args[value_count : value_count + order_count])
        return orders, list(expression.args[value_count + order_count :])

    def _window_order(self, order, expression, *, functions, aliases, window):
        column = self.evaluate(order, functions=functions, aliases=aliases, window=window)
        return (
            column if order.kind == "order" else (column.desc() if expression.data.get("descending") else column.asc())
        )

    def _window_bound(self, value, window):
        if value == "Window.currentRow":
            return window.currentRow
        if value == "Window.unboundedPreceding":
            return window.unboundedPreceding
        if value == "Window.unboundedFollowing":
            return window.unboundedFollowing
        return value

    def _bind_lambdas(self, expression: PySparkExpressionRecipe, columns):
        if expression.kind == "lambda_arg":
            name = expression.data.get("name")
            if name not in columns:
                return expression
            return PySparkExpressionRecipe(
                kind=expression.kind,
                type=expression.type,
                nullable=expression.nullable,
                data={**expression.data, "column": columns[name]},
                args=expression.args,
            )
        if expression.kind == "field" and "scope" not in expression.data:
            bound = self._bound_lambda_field(expression, columns)
            if bound is not None:
                return bound
        return PySparkExpressionRecipe(
            kind=expression.kind,
            type=expression.type,
            nullable=expression.nullable,
            data=expression.data,
            args=tuple(self._bind_lambdas(argument, columns) for argument in expression.args),
        )

    def _lambda_name(self, expression: PySparkExpressionRecipe) -> str:
        return str(expression.data.get("lambda_name", "item"))

    def _bound_lambda_field(self, expression: PySparkExpressionRecipe, columns):
        name_path = expression.data.get("name_path")
        if not isinstance(name_path, tuple) or not name_path:
            return None
        root = str(name_path[0])
        if root not in columns:
            return None
        column = columns[root]
        path = expression.data.get("path")
        segments = path if isinstance(path, tuple) else name_path[1:]
        for segment in segments:
            column = column.getField(str(segment))
        return PySparkExpressionRecipe(
            kind=expression.kind,
            type=expression.type,
            nullable=expression.nullable,
            data={**expression.data, "column": column},
            args=expression.args,
        )

    def _call(self, expression: PySparkExpressionRecipe, *, functions, aliases, window):
        function = expression.data["function"]
        args = [
            self.evaluate(argument, functions=functions, aliases=aliases, window=window) for argument in expression.args
        ]
        if function in {"lower", "ltrim", "rtrim", "trim", "upper"}:
            return getattr(functions, function)(args[0])
        if function in {"base64", "unbase64"}:
            return getattr(functions, function)(args[0])
        if function in {"encode", "decode"}:
            return getattr(functions, function)(args[0], expression.data["charset"])
        if function == "from_json":
            schema = self._schema.materialize()(cast(type, expression.data["schema"]))
            return functions.from_json(args[0], schema, expression.data["options"])
        if function == "from_csv":
            schema = self._ddl_schema(cast(type, expression.data["schema"]))
            return functions.from_csv(args[0], schema, expression.data["options"])
        if function in {"to_json", "to_csv"}:
            return getattr(functions, function)(args[0], expression.data["options"])
        if function == "coalesce":
            return functions.coalesce(*args)
        if function in {"nvl", "ifnull"}:
            return getattr(functions, function)(args[0], args[1])
        if function == "nvl2":
            return functions.nvl2(args[0], args[1], args[2])
        if function == "zeroifnull":
            return functions.zeroifnull(args[0])
        if function == "nullif":
            return functions.nullif(args[0], args[1])
        if function == "nanvl":
            return functions.nanvl(args[0], args[1])
        if function == "to_decimal":
            precision = expression.data["precision"]
            scale = expression.data["scale"]
            return args[0].cast(f"decimal({precision},{scale})")
        if function == "substring":
            return functions.substring(args[0], expression.data["start"], expression.data["length"])
        if function == "split":
            return functions.split(args[0], expression.data["pattern"], expression.data["limit"])
        if function == "regexp_replace":
            return functions.regexp_replace(args[0], expression.data["pattern"], expression.data["replacement"])
        if function == "regexp_extract":
            return functions.regexp_extract(args[0], expression.data["pattern"], expression.data["group"])
        if function == "length":
            return functions.length(args[0])
        if function in {"initcap", "reverse"}:
            return getattr(functions, function)(args[0])
        if function == "translate":
            return functions.translate(args[0], expression.data["matching"], expression.data["replacement"])
        if function == "instr":
            return functions.instr(args[0], expression.data["substring"])
        if function == "levenshtein":
            return functions.levenshtein(args[0], args[1])
        if function == "concat_ws":
            return functions.concat_ws(expression.data["separator"], *args)
        if function in {"hash", "xxhash64"}:
            return getattr(functions, function)(*args)
        if function in {"md5", "sha1"}:
            return getattr(functions, function)(args[0])
        if function == "sha2":
            return functions.sha2(args[0], expression.data["bits"])
        if function == "date_add":
            days = expression.data.get("days", args[1] if len(args) == 2 else None)
            return functions.date_add(args[0], days)
        if function == "date_sub":
            return functions.date_sub(args[0], expression.data["days"])
        if function == "datediff":
            return functions.datediff(args[0], args[1])
        if function == "date_trunc":
            return functions.date_trunc(expression.data["unit"], args[0])
        if function == "trunc":
            return functions.trunc(args[0], expression.data["unit"])
        if function in {"year", "month", "dayofmonth", "hour", "minute", "second"}:
            return getattr(functions, function)(args[0])
        if function in {"to_date", "to_timestamp"}:
            return (
                getattr(functions, function)(args[0], expression.data["format"])
                if "format" in expression.data
                else getattr(functions, function)(args[0])
            )
        if function == "abs":
            return functions.abs(args[0])
        if function == "round":
            return functions.round(args[0], expression.data["scale"])
        if function == "bround":
            return functions.bround(args[0], expression.data["scale"])
        if function in {"ceil", "floor"}:
            return getattr(functions, function)(args[0])
        if function in {"sqrt", "exp", "signum"}:
            return getattr(functions, function)(args[0])
        if function == "pow":
            return functions.pow(args[0], args[1])
        if function == "log":
            return (
                functions.log(expression.data["base"], args[0]) if "base" in expression.data else functions.log(args[0])
            )
        raise TypeError(f"Unsupported PySpark helper call: {function}")

    def _python_udf(self, expression: PySparkExpressionRecipe, *, functions, aliases, window):
        args = [
            self.evaluate(argument, functions=functions, aliases=aliases, window=window) for argument in expression.args
        ]
        return_type = expression.data["return_type"]
        if not expression.data.get("pyspark_return_type"):
            return_type = self._schema.materialize().type(cast(StructureType, return_type))
        udf = functions.udf(expression.data["function"], returnType=return_type)
        return udf(*args)

    def _int_data(self, expression: PySparkExpressionRecipe, key: str, default: int) -> int:
        value = expression.data.get(key, default)
        return value if isinstance(value, int) else default

    def _binary(self, expression: PySparkExpressionRecipe, *, functions, aliases, window, operator: str):
        left, right = (
            self.evaluate(argument, functions=functions, aliases=aliases, window=window) for argument in expression.args
        )
        if operator == "and":
            return left & right
        if operator == "or":
            return left | right
        if operator == "eq":
            return left == right
        if operator == "ne":
            return left != right
        if operator == "gt":
            return left > right
        if operator == "lt":
            return left < right
        if operator == "le":
            return left <= right
        if operator == "ge":
            return left >= right
        if operator == "add":
            return left + right
        if operator == "sub":
            return left - right
        if operator == "mul":
            return left * right
        if operator == "div":
            return left / right
        if operator == "mod":
            return left % right
        raise TypeError(f"Unsupported PySpark binary operator: {operator}")

    def _ddl_schema(self, schema: Any) -> str:
        return ", ".join(
            f"{self._ddl_field(field.column)} {self._ddl_type(field.type)}"
            for field in schema._structure_fields.values()
        )

    def _ddl_field(self, name: str) -> str:
        if name.replace("_", "a").isalnum() and not name[:1].isdigit():
            return name
        return f"`{name.replace('`', '``')}`"

    def _ddl_type(self, type: StructureType) -> str:
        if isinstance(type, StringType):
            return "STRING"
        if isinstance(type, BinaryType):
            return "BINARY"
        if isinstance(type, IntegerType):
            return "INT"
        if isinstance(type, LongType):
            return "BIGINT"
        if isinstance(type, FloatType):
            return "FLOAT"
        if isinstance(type, DoubleType):
            return "DOUBLE"
        if isinstance(type, BooleanType):
            return "BOOLEAN"
        if isinstance(type, DateType):
            return "DATE"
        if isinstance(type, TimestampType):
            return "TIMESTAMP"
        if isinstance(type, DecimalType):
            return f"DECIMAL({type.precision},{type.scale})"
        if isinstance(type, ArrayType):
            return f"ARRAY<{self._ddl_type(type.element)}>"
        if isinstance(type, MapType):
            return f"MAP<{self._ddl_type(type.key)},{self._ddl_type(type.value)}>"
        if isinstance(type, StructType):
            fields = ",".join(
                f"{self._ddl_field(field.column)}:{self._ddl_type(field.type)}"
                for field in type.schema._structure_fields.values()
            )
            return f"STRUCT<{fields}>"
        raise TypeError(f"Unsupported Structure type: {type!r}")

    def _bitwise_binary(self, expression: PySparkExpressionRecipe, *, functions, aliases, window):
        left, right = (
            self.evaluate(argument, functions=functions, aliases=aliases, window=window) for argument in expression.args
        )
        method = {
            "bitwise_and": "bitwiseAND",
            "bitwise_or": "bitwiseOR",
            "bitwise_xor": "bitwiseXOR",
        }[expression.kind]
        return getattr(left, method)(right)
