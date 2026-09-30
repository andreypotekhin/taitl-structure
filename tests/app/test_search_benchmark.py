import json

from scripts.benchmark_search_artifact_reuse import read_artifact_measurements
from scripts.benchmark_search_boundaries import read_measurements, save_summary
from scripts.benchmark_search_repeat_queries import command_for, read_repeat_measurements, summarize


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


def test_repeat_measurements_parse_preparation_and_per_request_samples(tmp_path):
    log = tmp_path / "repeat.log"
    log.write_text(
        '[search-repeat] {"mode": "prepared-handwritten", "preparation_seconds": 1.5, "workload": "cached-score-hit"}\n'
        '[search-repeat] {"mode": "online", "request_index": 0, "construction_seconds": 2.0, '
        '"collection_seconds": 0.25, "workload": "cached-score-hit"}\n'
        "======= 1 passed in 3.00s =======\n",
        encoding="utf-8",
    )

    assert read_repeat_measurements(log) == {
        "preparation_seconds": 1.5,
        "requests": [
            {
                "mode": "online",
                "request_index": 0,
                "construction_seconds": 2.0,
                "collection_seconds": 0.25,
                "workload": "cached-score-hit",
            }
        ],
        "test_result": "1 passed",
        "pytest_seconds": 3.0,
        "heap_exhaustion": False,
    }


def test_repeat_summary_uses_only_successful_cohorts():
    records = [
        {
            "backend": "pyspark35",
            "variant": "cached-score-hit",
            "exit_code": status,
            "measurements": {
                "preparation_seconds": 4.0,
                "requests": [{"mode": "online", "construction_seconds": runtime, "collection_seconds": 0.2}],
            },
        }
        for status, runtime in ((0, 20.0), (1, 100.0), (0, 30.0))
    ]

    summary = summarize(records)
    online = next(record for record in summary if record["mode"] == "online")
    assert online == {
        "backend": "pyspark35",
        "variant": "cached-score-hit",
        "boundary_cleanup": "session-close",
        "mode": "online",
        "request_samples": 2,
        "median_construction_seconds": 25.0,
        "median_collection_seconds": 0.2,
        "median_preparation_seconds": None,
    }


def test_repeat_summary_keeps_score_cache_misses_separate():
    records = [
        {
            "backend": "pyspark40",
            "variant": variant,
            "exit_code": 0,
            "measurements": {
                "preparation_seconds": None,
                "requests": [
                    {"mode": "online", "construction_seconds": runtime, "collection_seconds": 0.2}
                ],
            },
        }
        for variant, runtime in (("cached-score-hit", 1.0), ("score-cache-miss", 9.0))
    ]

    summaries = summarize(records)

    assert [
        (item["variant"], item["median_construction_seconds"])
        for item in summaries
        if item["mode"] == "online"
    ] == [
        ("cached-score-hit", 1.0),
        ("score-cache-miss", 9.0),
    ]
    assert {item["mode"] for item in summaries if item["variant"] == "score-cache-miss"} == {"online", "generated"}


def test_repeat_summary_keeps_boundary_cleanup_strategies_separate():
    records = [
        {
            "backend": "pyspark35",
            "variant": "score-cache-miss",
            "boundary_cleanup": lifecycle,
            "exit_code": 0,
            "measurements": {
                "preparation_seconds": None,
                "requests": [{"mode": "online", "construction_seconds": runtime, "collection_seconds": 0.2}],
            },
        }
        for lifecycle, runtime in (("session-close", 8.0), ("after-collect", 2.0))
    ]

    summaries = summarize(records)

    online = [item for item in summaries if item["mode"] == "online"]
    assert [(item["boundary_cleanup"], item["median_construction_seconds"]) for item in online] == [
        ("session-close", 8.0),
        ("after-collect", 2.0),
    ]


def test_score_cache_miss_runner_passes_only_structure_execution_modes():
    command = command_for("pyspark40", "score-cache-miss", 1, ("online", "generated"), 900)

    assert "STRUCTURE_SEARCH_REPEAT_WORKLOAD=score-cache-miss" in command
    assert "STRUCTURE_SEARCH_REPEAT_MODE_ORDER=online,generated" in command
    assert "STRUCTURE_PROFILE_QUERY_PLANS=off" in command
