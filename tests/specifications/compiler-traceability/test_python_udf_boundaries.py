from structure.plugin.pyspark.compiler.logic.traceability.FindPythonUdfBoundaries import FindPythonUdfBoundaries
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


def test_python_udf_boundary_finder_reports_nested_udfs_once_per_step() -> None:
    udf = PySparkExpressionRecipe(
        kind="python_udf",
        type=None,
        nullable=True,
        data={"function_name": "normalize"},
    )
    expression = PySparkExpressionRecipe(kind="add", type=None, nullable=True, data={}, args=(udf, udf))

    boundaries = FindPythonUdfBoundaries()(step="clean", schema="CleanRow", expressions=(expression,))

    assert [(boundary.step, boundary.hook, boundary.phase, boundary.schema) for boundary in boundaries] == [
        ("clean", "normalize", "expression", "CleanRow")
    ]
