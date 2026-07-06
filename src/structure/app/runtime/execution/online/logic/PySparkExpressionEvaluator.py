from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


class PySparkExpressionEvaluator:

    def evaluate(self, expression: PySparkExpressionRecipe, *, functions, aliases, window=None):
        if expression.kind == "field":
            scope = str(expression.data["scope"])
            field = str(expression.data["field"])
            alias = aliases.get(scope, scope)
            return functions.col(f"{alias}.{field}")
        if expression.kind == "literal":
            return functions.lit(expression.data["value"])
        if expression.kind == "lambda_arg":
            return expression.data["column"]
        if expression.kind == "call":
            return self._call(expression, functions=functions, aliases=aliases, window=window)
        if expression.kind == "reserved_v2":
            return self._reserved(expression, functions=functions, aliases=aliases, window=window)
        if expression.kind == "is_not_null":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window).isNotNull()
        if expression.kind == "is_null":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window).isNull()
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
        if expression.kind == "when":
            condition, value, fallback = (
                self.evaluate(argument, functions=functions, aliases=aliases, window=window)
                for argument in expression.args
            )
            return functions.when(condition, value).otherwise(fallback)
        if expression.kind == "null_safe_eq":
            left, right = expression.args
            return self.evaluate(left, functions=functions, aliases=aliases, window=window).eqNullSafe(
                self.evaluate(right, functions=functions, aliases=aliases, window=window)
            )
        if expression.kind == "not":
            return ~self.evaluate(expression.args[0], functions=functions, aliases=aliases, window=window)
        raise TypeError(f"Unsupported PySpark expression recipe: {expression.kind}")

    def _reserved(self, expression: PySparkExpressionRecipe, *, functions, aliases, window):
        function = expression.data["function"]
        if function == "array_transform":
            array, body = expression.args
            return functions.transform(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                lambda item: self.evaluate(
                    self._bind_lambdas(body, {"item": item}), functions=functions, aliases=aliases, window=window
                ),
            )
        if function == "array_filter":
            array, body = expression.args
            return functions.filter(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                lambda item: self.evaluate(
                    self._bind_lambdas(body, {"item": item}), functions=functions, aliases=aliases, window=window
                ),
            )
        if function == "array_exists":
            array, body = expression.args
            return functions.exists(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                lambda item: self.evaluate(
                    self._bind_lambdas(body, {"item": item}), functions=functions, aliases=aliases, window=window
                ),
            )
        if function == "array_forall":
            array, body = expression.args
            return functions.forall(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                lambda item: self.evaluate(
                    self._bind_lambdas(body, {"item": item}), functions=functions, aliases=aliases, window=window
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
            aggregate = functions.aggregate(
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
                return aggregate
            return functions.transform(aggregate, lambda acc: self.evaluate(
                self._bind_lambdas(finished, {"acc": acc}), functions=functions, aliases=aliases, window=window
            ))
        if function == "array_sort_by":
            [array] = expression.args
            return functions.sort_array(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                asc=not expression.data.get("descending"),
            )
        if function == "array_flatten":
            [array] = expression.args
            return functions.flatten(self.evaluate(array, functions=functions, aliases=aliases, window=window))
        if function == "array_distinct":
            [array] = expression.args
            return functions.array_distinct(self.evaluate(array, functions=functions, aliases=aliases, window=window))
        if function == "array_position":
            array, item = expression.args
            return functions.array_position(
                self.evaluate(array, functions=functions, aliases=aliases, window=window),
                self.evaluate(item, functions=functions, aliases=aliases, window=window),
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
            return getattr(functions, name)(
                self.evaluate(value, functions=functions, aliases=aliases, window=window)
            )
        if function in {"window_row_number", "window_rank", "window_dense_rank"}:
            order_by, *partition_by = expression.args
            return getattr(functions, function.removeprefix("window_"))().over(
                self._window(order_by, partition_by, expression, functions=functions, aliases=aliases, window=window)
            )
        if function in {"window_percent_rank", "window_cume_dist"}:
            order_by, *partition_by = expression.args
            return getattr(functions, function.removeprefix("window_"))().over(
                self._window(order_by, partition_by, expression, functions=functions, aliases=aliases, window=window)
            )
        if function == "window_ntile":
            order_by, *partition_by = expression.args
            return functions.ntile(expression.data["buckets"]).over(
                self._window(order_by, partition_by, expression, functions=functions, aliases=aliases, window=window)
            )
        if function in {"window_lag", "window_lead"}:
            value, order_by, *partition_by = expression.args
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
            value, order_by, *partition_by = expression.args
            column = self.evaluate(value, functions=functions, aliases=aliases, window=window)
            return getattr(functions, function.removeprefix("window_rolling_"))(column).over(
                self._window(order_by, partition_by, expression, functions=functions, aliases=aliases, window=window)
            )
        if function in {"window_first_value", "window_last_value", "window_nth_value"}:
            count = self._int_data(expression, "value_count", 1)
            values = expression.args[:count]
            order_by = expression.args[count]
            partition_by = list(expression.args[count + 1:])
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
            "window_count_distinct",
        }:
            count = self._int_data(expression, "value_count", 1)
            values = expression.args[:count]
            order_by = expression.args[count]
            partition_by = list(expression.args[count + 1:])
            name = "countDistinct" if function == "window_count_distinct" else function.removeprefix("window_")
            argument = (
                self.evaluate(values[0], functions=functions, aliases=aliases, window=window)
                if values
                else functions.lit(1)
            )
            return getattr(functions, name)(argument).over(
                self._window(order_by, partition_by, expression, functions=functions, aliases=aliases, window=window)
            )
        raise TypeError(f"Unsupported PySpark reserved expression: {function}")

    def _window(self, order_by, partition_by, expression, *, functions, aliases, window):
        if window is None:
            raise TypeError("Window expression evaluation requires a PySpark Window module")
        partitions = [
            self.evaluate(partition, functions=functions, aliases=aliases, window=window)
            for partition in partition_by
        ]
        order = self.evaluate(order_by, functions=functions, aliases=aliases, window=window)
        ordering = order.desc() if expression.data.get("descending") else order.asc()
        spec = window.partitionBy(*partitions).orderBy(ordering)
        if "frame_kind" in expression.data:
            frame = spec.rowsBetween if expression.data["frame_kind"] == "rows" else spec.rangeBetween
            return frame(
                self._window_bound(expression.data["frame_start"], window),
                self._window_bound(expression.data["frame_end"], window),
            )
        if "preceding" in expression.data:
            return spec.rowsBetween(-int(expression.data["preceding"]), 0)
        return spec

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
        return PySparkExpressionRecipe(
            kind=expression.kind,
            type=expression.type,
            nullable=expression.nullable,
            data=expression.data,
            args=tuple(self._bind_lambdas(argument, columns) for argument in expression.args),
        )

    def _call(self, expression: PySparkExpressionRecipe, *, functions, aliases, window):
        function = expression.data["function"]
        args = [
            self.evaluate(argument, functions=functions, aliases=aliases, window=window)
            for argument in expression.args
        ]
        if function == "lower":
            return functions.lower(args[0])
        if function == "trim":
            return functions.trim(args[0])
        if function == "upper":
            return functions.upper(args[0])
        if function == "coalesce":
            return functions.coalesce(*args)
        if function == "to_decimal":
            precision = expression.data["precision"]
            scale = expression.data["scale"]
            return args[0].cast(f"decimal({precision},{scale})")
        raise TypeError(f"Unsupported PySpark helper call: {function}")

    def _int_data(self, expression: PySparkExpressionRecipe, key: str, default: int) -> int:
        value = expression.data.get(key, default)
        return value if isinstance(value, int) else default

    def _binary(self, expression: PySparkExpressionRecipe, *, functions, aliases, window, operator: str):
        left, right = (
            self.evaluate(argument, functions=functions, aliases=aliases, window=window)
            for argument in expression.args
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
        raise TypeError(f"Unsupported PySpark binary operator: {operator}")
