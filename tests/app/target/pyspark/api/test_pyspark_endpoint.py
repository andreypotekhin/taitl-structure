from structure.core.target.pyspark.api import PySpark
from structure.core.target.pyspark.api.Files import CompareGeneratedFiles
from structure.core.target.pyspark.api.Plan import LowerPySparkPlan
from structure.core.target.pyspark.api.Render import RenderPySparkProject
from structure.core.target.pyspark.api.Schema import MaterializePySparkSchema


def test_pyspark_endpoint_groups_commands_and_creates_fresh_actions() -> None:
    assert isinstance(PySpark.plan.lower(), LowerPySparkPlan)
    assert isinstance(PySpark.schema.materialize(), MaterializePySparkSchema)
    assert isinstance(PySpark.render.project(), RenderPySparkProject)
    assert isinstance(PySpark.files.compare(), CompareGeneratedFiles)
    assert PySpark.plan.lower() is not PySpark.plan.lower()
