from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


class PySparkExpressionEvaluator:

    def evaluate(self, expression: PySparkExpressionRecipe, *, functions, aliases):
        if expression.kind == "field":
            scope = str(expression.data["scope"])
            field = str(expression.data["field"])
            alias = aliases.get(scope, scope)
            return functions.col(f"{alias}.{field}")
        if expression.kind == "literal":
            return functions.lit(expression.data["value"])
        if expression.kind == "call":
            return self._call(expression, functions=functions, aliases=aliases)
        if expression.kind == "is_not_null":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases).isNotNull()
        if expression.kind == "is_null":
            return self.evaluate(expression.args[0], functions=functions, aliases=aliases).isNull()
        if expression.kind == "and":
            return self._binary(expression, functions=functions, aliases=aliases, operator="and")
        if expression.kind == "or":
            return self._binary(expression, functions=functions, aliases=aliases, operator="or")
        if expression.kind == "eq":
            return self._binary(expression, functions=functions, aliases=aliases, operator="eq")
        if expression.kind == "ne":
            return self._binary(expression, functions=functions, aliases=aliases, operator="ne")
        if expression.kind == "gt":
            return self._binary(expression, functions=functions, aliases=aliases, operator="gt")
        if expression.kind == "sub":
            return self._binary(expression, functions=functions, aliases=aliases, operator="sub")
        if expression.kind == "null_safe_eq":
            left, right = expression.args
            return self.evaluate(left, functions=functions, aliases=aliases).eqNullSafe(
                self.evaluate(right, functions=functions, aliases=aliases)
            )
        if expression.kind == "not":
            return ~self.evaluate(expression.args[0], functions=functions, aliases=aliases)
        raise TypeError(f"Unsupported PySpark expression recipe: {expression.kind}")

    def _call(self, expression: PySparkExpressionRecipe, *, functions, aliases):
        function = expression.data["function"]
        args = [self.evaluate(argument, functions=functions, aliases=aliases) for argument in expression.args]
        if function == "lower":
            return functions.lower(args[0])
        if function == "trim":
            return functions.trim(args[0])
        if function == "coalesce":
            return functions.coalesce(*args)
        if function == "to_decimal":
            precision = expression.data["precision"]
            scale = expression.data["scale"]
            return args[0].cast(f"decimal({precision},{scale})")
        raise TypeError(f"Unsupported PySpark helper call: {function}")

    def _binary(self, expression: PySparkExpressionRecipe, *, functions, aliases, operator: str):
        left, right = (self.evaluate(argument, functions=functions, aliases=aliases) for argument in expression.args)
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
        if operator == "sub":
            return left - right
        raise TypeError(f"Unsupported PySpark binary operator: {operator}")
