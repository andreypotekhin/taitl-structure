from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import render_pyspark_expression


class RenderPySparkFilters:
    """Render a step's consecutive predicates as one DataFrame filter."""

    def __call__(
        self,
        filters: tuple[PySparkExpressionRecipe, ...],
        *,
        scope_aliases: dict[str, str],
        target: str,
    ) -> list[str]:
        predicate = " & ".join(
            f"({render_pyspark_expression(filter, scope_aliases=scope_aliases)})" for filter in filters
        )
        return [f"        {target} = {target}.where({predicate})"]
