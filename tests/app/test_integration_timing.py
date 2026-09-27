import pytest
from integration.pyspark.support.timing import phase


def test_phase_reports_elapsed_time_and_preserves_exception(capsys) -> None:
    with pytest.raises(RuntimeError, match="boom"):
        with phase("test phase"):
            raise RuntimeError("boom")

    output = capsys.readouterr().out
    assert "[phase] test phase: starting" in output
    assert "[phase] test phase: " in output
    assert output.rstrip().endswith("s")
