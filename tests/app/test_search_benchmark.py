import json

from scripts.benchmark_search_artifact_reuse import read_artifact_measurements
from scripts.benchmark_search_boundaries import read_measurements, save_summary


def test_measurements_keep_failed_test_totals_and_accumulate_phases(tmp_path):
    log = tmp_path / "failed.log"
    log.write_text(
        "[phase] online construction: starting\n"
        "[phase] online construction: 1.25s\n"
        "[phase] online construction: 2.00s\n"
        "[snapshot] rows: 0.50s\n"
        "java.lang.OutOfMemoryError: Java heap space\n"
        "======= 1 failed, 6 passed, 1 error in 123.45s (0:02:03) =======\n"
    )
    assert read_measurements(log) == {
        "phases": {"online construction": 3.25},
        "result": "1 failed, 6 passed, 1 error",
        "pytest_seconds": 123.45,
        "heap_exhaustion": True,
        "snapshot_seconds": 0.5,
    }


def test_summary_excludes_failed_runs_from_performance_medians(tmp_path):
    records = []
    for index, (status, total) in enumerate(((0, 20), (1, 100), (0, 30))):
        log = tmp_path / f"{index}.log"
        log.write_text(f"===== 1 {'failed' if status else 'passed'} in {total}.00s =====\n")
        records.append(
            dict(backend="pyspark35", case="rerank", policy="auto", exit_code=status, wall_seconds=total, log=str(log))
        )
    save_summary(tmp_path, records)
    (summary,) = json.loads((tmp_path / "medians.json").read_text())
    assert summary["runs"] == 3
    assert summary["passed"] == 2
    assert summary["median_pytest_seconds"] == 25


def test_artifact_measurements_parse_compile_records(tmp_path):
    log = tmp_path / "artifact-reuse.log"
    log.write_text(
        "[compile] source preparation examples.Transform: 1.25s hits=0 misses=1 outcome=success\n"
        "[compile] runtime examples.Transform: 0.05s hits=1 misses=0 outcome=success\n"
        "[compile] module total: 1.30s requests=2 hits=1 misses=1\n"
        "======= 1 passed in 2.50s (0:00:02) =======\n"
    )

    measurements = read_artifact_measurements(log)

    assert measurements["compilation"] == {
        "records": [
            {
                "phase": "source preparation",
                "transform": "examples.Transform",
                "elapsed": 1.25,
                "hits": 0,
                "misses": 1,
                "outcome": "success",
            },
            {
                "phase": "runtime",
                "transform": "examples.Transform",
                "elapsed": 0.05,
                "hits": 1,
                "misses": 0,
                "outcome": "success",
            },
        ],
        "by_phase": {
            "source preparation": {"requests": 1, "elapsed": 1.25, "hits": 0, "misses": 1},
            "runtime": {"requests": 1, "elapsed": 0.05, "hits": 1, "misses": 0},
        },
        "module_total": {"elapsed": 1.30, "requests": 2, "hits": 1, "misses": 1},
        "requests": 2,
        "elapsed": 1.3,
        "hits": 1,
        "misses": 1,
        "failures": 0,
    }
