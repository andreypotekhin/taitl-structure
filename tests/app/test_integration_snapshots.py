from unittest.mock import Mock

import pytest
from integration.pyspark.support.snapshots import Snapshots

from structure.core.runtime.session.model.TransformResult import TransformResult


def test_snapshots_use_new_relations_and_unique_paths(tmp_path) -> None:
    spark = Mock()
    frame = Mock()
    snapshots = Snapshots(spark, tmp_path)

    result = snapshots(frame, label="first")
    snapshots(frame, label="second")

    assert result is spark.read.parquet.return_value
    assert result is not frame
    assert [call.args[0] for call in frame.write.parquet.call_args_list] == [
        str(tmp_path / "1"),
        str(tmp_path / "2"),
    ]
    assert spark.read.parquet.call_args_list == frame.write.parquet.call_args_list


def test_output_snapshots_preserve_public_names_without_mutating_original(tmp_path) -> None:
    frame = Mock()
    spark = Mock()
    result = TransformResult({"items": frame}, schema={"items": "schema"})

    snapshot = Snapshots(spark, tmp_path).outputs(result, label="index")

    assert result["items"] is frame
    assert snapshot["items"] is spark.read.parquet.return_value
    assert snapshot.schema["items"] == result.schema["items"]
    assert dict(snapshot.stages) == {}


def test_failed_write_does_not_read_an_incomplete_snapshot(tmp_path) -> None:
    frame = Mock()
    spark = Mock()
    frame.write.parquet.side_effect = RuntimeError("write failed")

    with pytest.raises(RuntimeError, match="write failed"):
        Snapshots(spark, tmp_path)(frame, label="failed")

    spark.read.parquet.assert_not_called()
