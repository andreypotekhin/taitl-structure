from __future__ import annotations

import pytest
from integration.pyspark.support.backend_matrix import (
    assert_generated_connect_safe,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.rows import rows
from integration.pyspark.v1.support import multi_lookup

pytestmark = pytest.mark.integration


def test_results_match_online_and_generated(spark, tmp_path) -> None:
    generated_package = "integration_multi_generated"
    files = render_generated_project(
        multi_lookup.AddLookupProduct,
        source_transform=f"{multi_lookup.MODULE}.AddLookupProduct",
        generated_package=generated_package,
        source_schema_modules=multi_lookup.source_schema_modules(),
    )

    with generated_project(tmp_path, generated_package, files):
        schemas = multi_lookup.generated_schemas(generated_package)
        frames = multi_lookup.input_frames(spark, schemas)

        online = multi_lookup.AddLookupProduct(**frames).run(session(spark, execution_mode="online"))
        generated = multi_lookup.AddLookupProduct(**frames).run(
            session(spark, execution_mode="generated", generated_package=generated_package)
        )

        for name in ("accepted", "audited"):
            online_rows = rows(online[name], "id", recursive=False)
            generated_rows = rows(generated[name], "id", recursive=False)
            assert (
                online_rows
                == generated_rows
                == [
                    {"id": "o-1", "product_name": "Engine"},
                    {"id": "o-2", "product_name": None},
                ]
            )

    assert_generated_connect_safe(files)
