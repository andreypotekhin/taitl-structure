from structure.app.target.pyspark.api.FilesEndpoint import FilesEndpoint
from structure.app.target.pyspark.api.PlanEndpoint import PlanEndpoint
from structure.app.target.pyspark.api.RenderEndpoint import RenderEndpoint
from structure.app.target.pyspark.api.SchemaEndpoint import SchemaEndpoint


class PySparkEndpoint:

    def __init__(self) -> None:
        self.files = FilesEndpoint()
        self.plan = PlanEndpoint()
        self.render = RenderEndpoint()
        self.schema = SchemaEndpoint()
