"""I09272601: pruning preserves rows, stage access, and required validation."""

from pathlib import Path

import pytest
from integration.pyspark.support.backend_matrix import _plugin, generated_project

from structure import Schema, StructureConfig, StructureSession, Transform, input, lane, output, step, transform
from structure.plugin.pyspark import require_unique, string
from structure.plugin.pyspark.api.PySpark import PySpark

pytestmark = pytest.mark.integration
MODULE = "integration.pyspark.support.test_unused_steps"


class PruningRow(Schema):
    id = string(nullable=True)


class PrivateBranch(Transform):
    rows = input(PruningRow, streaming=True)
    unused = lane(PruningRow)
    result = output(PruningRow)

    @step(input=rows, output=unused)
    def private(self, row: PruningRow) -> PruningRow:
        return row

    @step(input=rows, output=result)
    def publish(self, row: PruningRow) -> PruningRow:
        return row


class CheckedBranch(PrivateBranch):
    @step(input=PrivateBranch.rows, output=PrivateBranch.unused)
    def private(self, row: PruningRow) -> PruningRow:
        require_unique(row.id)
        return row


def prepared(spark, subject, enabled, tmp_path):
    package = f"integration_pruning_{subject.__name__.lower()}_{int(enabled)}"
    config = StructureConfig.create(
        plugin=_plugin(), generated_package=package, prune_unused_steps=enabled,
        validate_intermediate=False, allow_stage_outputs=False,
    )
    artifact = subject.compile(config=config)
    files = PySpark.render.project()(
        artifact.pyspark_plan, source_transform=f"{MODULE}.{subject.__name__}", generated_package=package,
        source_schema_modules={MODULE: [PruningRow]},
        semantic_fingerprint=artifact.semantic_fingerprint,
    )
    return config, artifact, generated_project(tmp_path, package, files)


@pytest.mark.parametrize("subject, expected", [(PrivateBranch, 1), (CheckedBranch, 2)])
@pytest.mark.parametrize("rows", [[], [("a",)], [("a",), ("a",)]])
def test_pruning_batch_parity_and_unused_checks(spark, tmp_path, subject, expected, rows):
    from pyspark.sql import types as T

    frame = spark.createDataFrame(rows, T.StructType([T.StructField("id", T.StringType(), True)]))
    observed = []
    for enabled in (False, True):
        config, artifact, generated = prepared(spark, subject, enabled, tmp_path)
        assert len(artifact.pyspark_plan.steps) == (expected if enabled else 2)
        with generated:
            for mode in ("online", "generated"):
                from dataclasses import replace

                session = StructureSession(spark=spark, config=replace(config, execution_mode=mode))
                result = subject(rows=frame).run(session)
                assert list(result) == ["result"]
                # The unused assertion stays lazy, exactly as before pruning; no new action forces it.
                observed.append((result.result.schema, result.result.orderBy("id").collect()))
    assert all(item == observed[0] for item in observed)


def test_pruning_streaming_frames_remain_streaming_without_actions(spark, tmp_path):
    from dataclasses import replace

    from pyspark.sql import types as T

    source = Path(tmp_path) / "input"
    source.mkdir()
    frame = spark.readStream.schema(T.StructType([T.StructField("id", T.StringType(), True)])).json(str(source))
    config, artifact, generated = prepared(spark, PrivateBranch, True, tmp_path)
    assert len(artifact.pyspark_plan.steps) == 1
    with generated:
        for mode in ("online", "generated"):
            session = StructureSession(spark=spark, config=replace(config, execution_mode=mode))
            result = PrivateBranch(rows=frame).run(session)
            assert result.result.isStreaming
