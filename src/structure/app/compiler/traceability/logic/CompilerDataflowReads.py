from collections.abc import Iterable

from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


class CompilerDataflowReads:

    def reads(self, expression: PySparkExpressionRecipe) -> tuple[str, ...]:
        if expression.kind == "field":
            return (f"{expression.data['scope']}.{expression.data['field']}",)
        return self._unique(read for argument in expression.args for read in self.reads(argument))

    def _unique(self, values: Iterable[str]) -> tuple[str, ...]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            if value not in seen:
                result.append(value)
                seen.add(value)
        return tuple(result)
