"""Run sequential Search compiled-artifact reuse comparisons in fresh Compose runners."""

import argparse
import json
import re
import subprocess
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any

try:
    from scripts.benchmark_search_boundaries import CASES, read_measurements
except ModuleNotFoundError:
    from benchmark_search_boundaries import CASES, read_measurements  # type: ignore[import-not-found, no-redef]


BACKENDS = ("pyspark35", "pyspark40", "spark-connect40")
POLICIES = ("off", "module")
COMPILE_RECORD = re.compile(
    r"\[compile\] (?P<phase>.+) (?P<transform>[^:\r\n]+): (?P<elapsed>[0-9.]+)s "
    r"hits=(?P<hits>\d+) misses=(?P<misses>\d+) outcome=(?P<outcome>\w+)"
)
MODULE_TOTAL = re.compile(
    r"\[compile\] module total: (?P<elapsed>[0-9.]+)s requests=(?P<requests>\d+) "
    r"hits=(?P<hits>\d+) misses=(?P<misses>\d+)"
)


def read_compilation(path: Path) -> dict[str, Any]:
    content = path.read_text()
    records: list[dict[str, Any]] = [
        {
            "phase": match["phase"],
            "transform": match["transform"],
            "elapsed": float(match["elapsed"]),
            "hits": int(match["hits"]),
            "misses": int(match["misses"]),
            "outcome": match["outcome"],
        }
        for match in COMPILE_RECORD.finditer(content)
    ]
    by_phase: dict[str, dict[str, Any]] = {}
    for record in records:
        phase = record["phase"]
        totals = by_phase.setdefault(phase, {"requests": 0, "elapsed": 0.0, "hits": 0, "misses": 0})
        totals["requests"] += 1
        totals["elapsed"] += record["elapsed"]
        totals["hits"] += record["hits"]
        totals["misses"] += record["misses"]
    total_match = MODULE_TOTAL.search(content)
    module_total = (
        {
            "elapsed": float(total_match["elapsed"]),
            "requests": int(total_match["requests"]),
            "hits": int(total_match["hits"]),
            "misses": int(total_match["misses"]),
        }
        if total_match
        else None
    )
    return {
        "records": records,
        "by_phase": by_phase,
        "module_total": module_total,
        "requests": len(records),
        "elapsed": sum(record["elapsed"] for record in records),
        "hits": sum(record["hits"] for record in records),
        "misses": sum(record["misses"] for record in records),
        "failures": sum(record["outcome"] == "failure" for record in records),
    }


def read_artifact_measurements(path: Path) -> dict[str, Any]:
    measurements = read_measurements(path)
    measurements["compilation"] = read_compilation(path)
    return measurements


def command_for(backend: str, case: str, policy: str, timeout: int) -> list[str]:
    return [
        "docker",
        "compose",
        "--env-file",
        "infra/compose/.env",
        "-f",
        "infra/compose/docker-compose.yaml",
        "-p",
        "structure-integration",
        "run",
        "--rm",
        "-e",
        f"STRUCTURE_COMPILED_ARTIFACT_REUSE={policy}",
        "-e",
        "STRUCTURE_PROFILE_COMPILATION=1",
        "-e",
        "STRUCTURE_PROFILE_QUERY_PLANS=1",
        "-e",
        "STRUCTURE_PLAN_BOUNDARIES=auto",
        "-e",
        "STRUCTURE_SEARCH_STAGE_OUTPUTS=0",
        "-e",
        f"STRUCTURE_INTEGRATION_TIMEOUT={timeout}",
        "-e",
        f"INTEGRATION_PYTEST_ARGS=-k {CASES[case]} -vv -s --durations=20",
        f"structure-integration-{backend}",
        "bash",
        "/workspace/infra/compose/images/pyspark/run-integration.sh",
        backend,
    ]


def run(args: argparse.Namespace) -> list[dict[str, Any]]:
    root = Path(__file__).resolve().parents[1]
    records: list[dict[str, Any]] = []
    for backend in args.backends:
        for case in args.cases:
            timeout = args.full_timeout if case == "module" else args.timeout
            for repetition in range(1, args.repetitions + 1):
                selected_policies = tuple(args.policies)
                order = selected_policies if repetition % 2 else tuple(reversed(selected_policies))
                for policy in order:
                    name = f"artifact-reuse-{backend}-{case}-{policy}-{repetition}"
                    path = args.output / f"{name}.log"
                    if path.exists():
                        raise FileExistsError(f"Refusing to overwrite evidence: {path}")
                    command = command_for(backend, case, policy, timeout)
                    print(f"Starting {name}", flush=True)
                    started = perf_counter()
                    with path.open("w") as log:
                        result = subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
                    record = {
                        "backend": backend,
                        "case": case,
                        "policy": policy,
                        "repetition": repetition,
                        "exit_code": result.returncode,
                        "wall_seconds": round(perf_counter() - started, 2),
                        "log": str(path),
                        **read_artifact_measurements(path),
                    }
                    records.append(record)
                    (args.output / "results.json").write_text(json.dumps(records, indent=2) + "\n")
                    compilation = record["compilation"]
                    print(
                        json.dumps(
                            {
                                "backend": backend,
                                "case": case,
                                "policy": policy,
                                "repetition": repetition,
                                "exit_code": record["exit_code"],
                                "wall_seconds": record["wall_seconds"],
                                "pytest_seconds": record["pytest_seconds"],
                                "compile": {
                                    "requests": compilation["requests"],
                                    "hits": compilation["hits"],
                                    "misses": compilation["misses"],
                                    "elapsed": compilation["elapsed"],
                                },
                                "heap_exhaustion": record["heap_exhaustion"],
                                "result": record["result"],
                                "log": record["log"],
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
    save_summary(args.output, records)
    return records


def save_summary(output: Path, records: list[dict[str, Any]]) -> None:
    summaries = []
    for backend, case, policy in dict.fromkeys((r["backend"], r["case"], r["policy"]) for r in records):
        selected = [r for r in records if (r["backend"], r["case"], r["policy"]) == (backend, case, policy)]
        successful = [r for r in selected if r["exit_code"] == 0 and r["pytest_seconds"] is not None]
        summaries.append(
            {
                "backend": backend,
                "case": case,
                "policy": policy,
                "runs": len(selected),
                "passed": len(successful),
                "heap_failures": sum(r["heap_exhaustion"] for r in selected),
                "median_wall_seconds": median(r["wall_seconds"] for r in successful) if successful else None,
                "median_pytest_seconds": median(r["pytest_seconds"] for r in successful) if successful else None,
                "median_construction_seconds": (
                    median(
                        sum(duration for label, duration in r["phases"].items() if label.endswith(" construction"))
                        for r in successful
                    )
                    if successful
                    else None
                ),
                "median_compilation_seconds": (
                    median(r["compilation"]["elapsed"] for r in successful) if successful else None
                ),
                "median_compile_requests": (
                    median(r["compilation"]["requests"] for r in successful) if successful else None
                ),
                "median_compile_hits": median(r["compilation"]["hits"] for r in successful) if successful else None,
                "median_compile_misses": (
                    median(r["compilation"]["misses"] for r in successful) if successful else None
                ),
                "median_snapshot_seconds": median(r["snapshot_seconds"] for r in successful) if successful else None,
            }
        )
    (output / "medians.json").write_text(json.dumps(summaries, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backends", nargs="+", choices=BACKENDS, default=BACKENDS)
    parser.add_argument("--cases", nargs="+", choices=CASES, default=("rerank", "text", "module"))
    parser.add_argument("--policies", nargs="+", choices=POLICIES, default=POLICIES)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--full-timeout", type=int, default=1800)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.repetitions < 1 or args.timeout < 1 or args.full_timeout < 1:
        parser.error("repetitions and timeouts must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    records = run(args)
    if any(record["exit_code"] for record in records):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
