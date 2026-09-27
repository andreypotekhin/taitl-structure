import importlib

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_projects,
    session,
)

from examples.search.schemas.indexing.vector import VectorIndexPolicy
from examples.search.schemas.search import DocumentSearchCandidate
from examples.search.transforms.searching.search_docs.fuse import FuseDocuments
from structure import parameter

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(backend_name() == "spark-connect35", reason="Reliable checkpoint requires Connect 4.0"),
]
MODULE = "integration.pyspark.support.test_fusion_materialization"
PACKAGE = "integration_fusion_materialization_generated"


class MaterializedFusion(FuseDocuments):
    materialize = parameter(True)


@pytest.mark.parametrize("case", ["valid", "empty", "duplicate"])
def test_fusion_materialization_retains_rows_schemas_and_validation(spark, tmp_path, case):
    files = render_generated_projects(
        [(FuseDocuments, f"{FuseDocuments.__module__}.FuseDocuments"), (MaterializedFusion, f"{MODULE}.MaterializedFusion")],
        generated_package=PACKAGE,
        source_schema_modules={
            DocumentSearchCandidate.__module__: [DocumentSearchCandidate],
            VectorIndexPolicy.__module__: [VectorIndexPolicy],
        },
    )
    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.search")
        vector = importlib.import_module(f"{PACKAGE}.pyspark.schemas.vector")
        row = ("q", None, None, None, "query", 1, "d", "title", None,
               2.0, 2.0, 0.0, 0.0, 1.0, 0.0, None, None, None, 0.0, 60, None)
        data = [] if case == "empty" else [row] * (2 if case == "duplicate" else 1)
        inputs = dict(
            lexical_candidates=spark.createDataFrame(data, schemas.DOCUMENT_SEARCH_CANDIDATE_SCHEMA),
            vector_candidates=spark.createDataFrame([], schemas.DOCUMENT_SEARCH_CANDIDATE_SCHEMA),
            policy=spark.createDataFrame([("model", 3, "revision", "experiment", 10, 60)], vector.VECTOR_INDEX_POLICY_SCHEMA),
        )
        observed = []
        for transform in (FuseDocuments, MaterializedFusion):
            for mode in ("online", "generated"):
                def run():
                    result = transform(**inputs).run(session(spark, execution_mode=mode, generated_package=PACKAGE))
                    frame = result.candidates
                    assert frame.columns == schemas.DOCUMENT_SEARCH_CANDIDATE_SCHEMA.fieldNames()
                    return frame.schema, frame.orderBy("document_id").collect()

                if case == "duplicate":
                    with pytest.raises(Exception, match="REL-E0702"):
                        run()
                else:
                    observed.append(run())
        if observed:
            assert all(result == observed[0] for result in observed)
            assert len(observed[0][1]) == (0 if case == "empty" else 1)
