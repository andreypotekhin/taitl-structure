"""Run sequential, bounded Search boundary comparisons in fresh Compose runners."""

import argparse
import json
import re
import subprocess
from pathlib import Path
from statistics import median
from time import perf_counter


CASES = {
    "rerank": "test_document_search_reranks_bm25_candidates_for_multiple_queries",
    "text": "test_text_fixture_runs_online_and_generated",
    "module": "test_search.py",
    "boundaries": "test_connect_boundaries.py",
}
BACKENDS = ("pyspark35", "pyspark40", "spark-connect40")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backends", nargs="+", choices=BACKENDS, default=BACKENDS)
    parser.add_argument("--cases", nargs="+", choices=CASES, default=("rerank", "text"))
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summarize", action="store_true", help="Re-read existing logs without running Docker")
    args = parser.parse_args()
    if args.repetitions < 1 or args.timeout < 1:
        parser.error("repetitions and timeout must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    if args.summarize:
        save_summary(args.output, json.loads((args.output / "results.json").read_text()))
        return
    root = Path(__file__).resolve().parents[1]
    records: list[dict[str, object]] = []
    failures: dict[tuple[str, str, str], int] = {}
    for backend in args.backends:
        for case in args.cases:
            for repetition in range(1, args.repetitions + 1):
                for policy in ("off", "auto") if repetition % 2 else ("auto", "off"):
                    key = (backend, case, policy)
                    if failures.get(key, 0) >= 2:
                        continue
                    name = f"{backend}-{case}-{policy}-{repetition}"
                    path = args.output / f"{name}.log"
                    if path.exists():
                        raise FileExistsError(f"Refusing to overwrite evidence: {path}")
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
                        "-e",
                        f"STRUCTURE_PLAN_BOUNDARIES={policy}",
                        "-e",
                        "STRUCTURE_SEARCH_STAGE_OUTPUTS=0",
                        "-e",
                        f"STRUCTURE_INTEGRATION_TIMEOUT={args.timeout}",
                        "-e",
                        f"INTEGRATION_PYTEST_ARGS=-k {CASES[case]} -vv -s --durations=20",
                        f"structure-integration-{backend}",
                        "bash",
                        "/workspace/infra/compose/images/pyspark/run-integration.sh",
                        backend,
                    ]
                    print(f"Starting {name}", flush=True)
                    started = perf_counter()
                    with path.open("w") as log:
                        result = subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
                    record = dict(
                        backend=backend,
                        case=case,
                        policy=policy,
                        repetition=repetition,
                        exit_code=result.returncode,
                        wall_seconds=round(perf_counter() - started, 2),
                        log=str(path),
                        **read_measurements(path),
                    )
                    records.append(record)
                    if result.returncode:
                        failures[key] = failures.get(key, 0) + 1
                    (args.output / "results.json").write_text(json.dumps(records, indent=2) + "\n")
                    print(json.dumps(record), flush=True)
    save_summary(args.output, records)
    if any(record["exit_code"] for record in records):
        raise SystemExit(1)


def read_measurements(path):
    content = path.read_text()
    phases: dict[str, float] = {}
    for label, duration in re.findall(r"\[phase\] ([^\r\n]+): ([0-9.]+)s", content):
        phases[label] = phases.get(label, 0.0) + float(duration)
    totals = re.findall(r"=+ ([^\r\n]*\b(?:passed|failed|skipped|errors?)\b[^\r\n]*?) in ([0-9.]+)s", content)
    return dict(
        phases=phases,
        result=totals[-1][0] if totals else None,
        pytest_seconds=float(totals[-1][1]) if totals else None,
        heap_exhaustion="OutOfMemoryError" in content,
        snapshot_seconds=sum(float(duration) for duration in re.findall(r"\[snapshot\] [^\r\n]+: ([0-9.]+)s", content)),
    )


def save_summary(output, records):
    for record in records:
        record.update(read_measurements(Path(record["log"])))
    (output / "results.json").write_text(json.dumps(records, indent=2) + "\n")
    summaries = []
    for backend, case, policy in dict.fromkeys((r["backend"], r["case"], r["policy"]) for r in records):
        selected = [r for r in records if (r["backend"], r["case"], r["policy"]) == (backend, case, policy)]
        successful = [r for r in selected if r["exit_code"] == 0 and r["pytest_seconds"] is not None]
        summaries.append(
            dict(
                backend=backend,
                case=case,
                policy=policy,
                runs=len(selected),
                passed=len(successful),
                heap_failures=sum(r["heap_exhaustion"] for r in selected),
                median_wall_seconds=median(r["wall_seconds"] for r in successful) if successful else None,
                median_pytest_seconds=median(r["pytest_seconds"] for r in successful) if successful else None,
                median_construction_seconds=(
                    median(
                        sum(duration for label, duration in r["phases"].items() if label.endswith(" construction"))
                        for r in successful
                    )
                    if successful
                    else None
                ),
                median_snapshot_seconds=median(r["snapshot_seconds"] for r in successful) if successful else None,
                median_phases=(
                    {
                        label: median(r["phases"][label] for r in successful)
                        for label in successful[0]["phases"]
                        if all(label in r["phases"] for r in successful)
                    }
                    if successful
                    else {}
                ),
            )
        )
    (output / "medians.json").write_text(json.dumps(summaries, indent=2) + "\n")


if __name__ == "__main__":
    main()
