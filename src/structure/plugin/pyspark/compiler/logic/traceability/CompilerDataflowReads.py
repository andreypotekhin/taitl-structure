from collections.abc import Iterable
from typing import cast

from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationQueryRecipe import PySparkRelationQueryRecipe


class CompilerDataflowReads:

    def reads(self, expression: PySparkExpressionRecipe) -> tuple[str, ...]:
        if expression.kind == "field":
            if "scope" not in expression.data:
                return ()
            return (f"{expression.data['scope']}.{expression.data['field']}",)
        reads = [read for argument in expression.args for read in self.reads(argument)]
        query = cast(PySparkRelationQueryRecipe | None, (expression.data or {}).get("query"))
        if query is not None:
            for kind, value in query.operations:
                if kind == "where":
                    reads.extend(self.reads(cast(PySparkExpressionRecipe, value)))
                elif kind == "order_by":
                    reads.extend(
                        read
                        for order in cast(tuple[PySparkExpressionRecipe, ...], value)
                        for read in self.reads(order)
                    )
        return self._unique(reads)

    def _unique(self, values: Iterable[str]) -> tuple[str, ...]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            if value not in seen:
                result.append(value)
                seen.add(value)
        return tuple(result)
