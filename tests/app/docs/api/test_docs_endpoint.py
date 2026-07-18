import sys

from structure.core.docs.api import Docs, RenderStructureDocsProject


def test_docs_endpoint_groups_fresh_command_instances_without_spark_imports() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    assert isinstance(Docs.render.project(), RenderStructureDocsProject)
    assert Docs.render.project() is not Docs.render.project()

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
