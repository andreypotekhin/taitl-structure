"""Measure distinct cached-score Search requests in persistent integration sessions."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any

BACKENDS = ("pyspark35", "pyspark40", "spark-connect40")
MODES = ("online", "generated", "handwritten", "prepared-handwritten")
MODES_BY_VARIANT = {
    "cached-score-hit": MODES,
    "score-cache-miss": ("online", "generated"),
}
TEST = "test_document_search_reranks_bm25_candidates_for_multiple_queries"
SUMMARY = re.compile(r"=+ ([^\r\n]*\b(?:passed|failed|skipped|errors?)\b[^\r\n]*?) in ([0-9.]+)s")


def read_repeat_measurements(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    records = [
        json.loads(line.removeprefix("[search-repeat] "))
        for line in text.splitlines()
        if line.startswith("[search-repeat] {")
    ]
    summary = SUMMARY.findall(text)
    preparation = next(
        (record["preparation_seconds"] for record in records if "preparation_seconds" in record),
        None,
    )
    requests = [record for record in records if "request_index" in record]
    return {
        "preparation_seconds": preparation,
        "requests": requests,
        "test_result": summary[-1][0] if summary else None,
        "pytest_seconds": float(summary[-1][1]) if summary else None,
        "heap_exhaustion": "OutOfMemoryError" in text,
    }


def source_identity(root: Path) -> dict[str, Any]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()
    tracked = subprocess.run(
        ["git", "diff", "--name-only"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.splitlines()
    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    paths = sorted(set(tracked + untracked))
    hashes = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in paths}
    return {"commit": commit, "dirty_file_sha256": hashes}


def command_for(backend: str, variant: str, requests: int, mode_order: tuple[str, ...], timeout: int) -> list[str]:
    env = {
        "STRUCTURE_SEARCH_REPEAT_REQUESTS": str(requests),
        "STRUCTURE_SEARCH_REPEAT_MODE_ORDER": ",".join(mode_order),
        "STRUCTURE_SEARCH_REPEAT_WORKLOAD": variant,
        "STRUCTURE_SEARCH_RELEASE_BOUNDARIES": os.environ.get("STRUCTURE_SEARCH_RELEASE_BOUNDARIES", "false"),
        "STRUCTURE_PROFILE_QUERY_PLANS": os.environ.get("STRUCTURE_PROFILE_QUERY_PLANS", "off"),
        "STRUCTURE_PLAN_BOUNDARIES": "auto",
        "STRUCTURE_SEARCH_STAGE_OUTPUTS": "0",
        "STRUCTURE_PRUNE_UNUSED_STEPS": "false",
        "STRUCTURE_COMPILED_ARTIFACT_REUSE": "module",
        "STRUCTURE_INTEGRATION_TIMEOUT": str(timeout),
        "INTEGRATION_PYTEST_ARGS": f"-k {TEST} -vv -s --durations=20",
    }
    command = [
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
    ]
    for key, value in env.items():
        command.extend(("-e", f"{key}={value}"))
    command.extend(
        (
            f"structure-integration-{backend}",
            "bash",
            "/workspace/infra/compose/images/pyspark/run-integration.sh",
            backend,
        )
    )
    return command


def summarize(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries = []
    groups = dict.fromkeys(
        (
            record["backend"],
            record.get("variant", "cached-score-hit"),
            record.get("boundary_cleanup", "session-close"),
        )
        for record in records
    )
    for backend, variant, boundary_cleanup in groups:
        for mode in MODES_BY_VARIANT[variant]:
            samples = [
                request
                for record in records
                if record["backend"] == backend
                and record.get("variant", "cached-score-hit") == variant
                and record.get("boundary_cleanup", "session-close") == boundary_cleanup
                and record["exit_code"] == 0
                for request in record["measurements"]["requests"]
                if request["mode"] == mode
            ]
            preparations = [
                record["measurements"]["preparation_seconds"]
                for record in records
                if record["backend"] == backend
                and record.get("variant", "cached-score-hit") == variant
                and record.get("boundary_cleanup", "session-close") == boundary_cleanup
                and record["exit_code"] == 0
                and record["measurements"]["preparation_seconds"] is not None
            ]
            summaries.append(
                {
                    "backend": backend,
                    "variant": variant,
                    "boundary_cleanup": boundary_cleanup,
                    "mode": mode,
                    "request_samples": len(samples),
                    "median_construction_seconds": (
                        median(r["construction_seconds"] for r in samples) if samples else None
                    ),
                    "median_collection_seconds": median(r["collection_seconds"] for r in samples) if samples else None,
                    "median_preparation_seconds": (
                        median(preparations) if mode == "prepared-handwritten" and preparations else None
                    ),
                }
            )
    return summaries


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("all-supported", *BACKENDS), default="all-supported")
    parser.add_argument(
        "--variant", choices=("all-supported", *MODES_BY_VARIANT.keys()), default="all-supported"
    )
    parser.add_argument("--cohorts", type=int, default=3)
    parser.add_argument("--requests", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.cohorts, args.requests, args.timeout) < 1:
        parser.error("cohorts, requests, and timeout must be positive")

    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    identity = source_identity(root)
    backends = BACKENDS if args.backend == "all-supported" else (args.backend,)
    variants = tuple(MODES_BY_VARIANT) if args.variant == "all-supported" else (args.variant,)
    boundary_cleanup = (
        "after-collect"
        if os.environ.get("STRUCTURE_SEARCH_RELEASE_BOUNDARIES", "false").lower() == "true"
        else "session-close"
    )
    records: list[dict[str, Any]] = []
    for backend in backends:
        for variant in variants:
            modes = MODES_BY_VARIANT[variant]
            for cohort in range(1, args.cohorts + 1):
                mode_order = modes if cohort % 2 else tuple(reversed(modes))
                name = f"{variant}-{backend}-cohort-{cohort}"
                log = output / f"{name}.txt"
                if log.exists():
                    raise FileExistsError(f"Refusing to overwrite existing benchmark log: {log}")
                command = command_for(backend, variant, args.requests, mode_order, args.timeout)
                print(f"Starting {name} with mode order {mode_order}", flush=True)
                started = perf_counter()
                with log.open("w", encoding="utf-8") as sink:
                    result = subprocess.run(command, cwd=root, stdout=sink, stderr=subprocess.STDOUT, check=False)
                measurements: dict[str, Any] = read_repeat_measurements(log)
                record = {
                    "backend": backend,
                    "variant": variant,
                    "boundary_cleanup": boundary_cleanup,
                    "cohort": cohort,
                    "mode_order": mode_order,
                    "exit_code": result.returncode,
                    "wall_seconds": round(perf_counter() - started, 2),
                    "log": str(log),
                    "source_identity": identity,
                    "measurements": measurements,
                }
                records.append(record)
                (output / "results.json").write_text(json.dumps(records, indent=2) + "\n", encoding="utf-8")
                print(
                    json.dumps(
                        {
                            "backend": backend,
                            "variant": variant,
                            "cohort": cohort,
                            "exit_code": result.returncode,
                            "wall_seconds": record["wall_seconds"],
                            "pytest_seconds": measurements["pytest_seconds"],
                            "requests": len(measurements["requests"]),
                            "heap_exhaustion": measurements["heap_exhaustion"],
                            "log": str(log),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
    (output / "medians.json").write_text(json.dumps(summarize(records), indent=2) + "\n", encoding="utf-8")
    if any(record["exit_code"] for record in records):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
