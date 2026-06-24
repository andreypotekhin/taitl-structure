from structure.app.target.pyspark.api import pyspark
from structure.app.target.pyspark.api.FilesEndpoint import CompareGeneratedFiles
from structure.app.target.pyspark.api.PlanEndpoint import LowerPySparkPlan
from structure.app.target.pyspark.api.RenderEndpoint import RenderPySparkProject
from structure.app.target.pyspark.api.SchemaEndpoint import MaterializePySparkSchema


def test_pyspark_endpoint_groups_commands_and_creates_fresh_actions() -> None:
    assert isinstance(pyspark.plan.lower(), LowerPySparkPlan)
    assert isinstance(pyspark.schema.materialize(), MaterializePySparkSchema)
    assert isinstance(pyspark.render.project(), RenderPySparkProject)
    assert isinstance(pyspark.files.compare(), CompareGeneratedFiles)
    assert pyspark.plan.lower() is not pyspark.plan.lower()
