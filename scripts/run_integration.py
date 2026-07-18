from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from ensure_compose_env import main as ensure_compose_env

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "infra" / "compose" / "docker-compose.yaml"
ENV = ROOT / "infra" / "compose" / ".env"
WORKSPACE_TMP = ROOT / ".pytest-workspace-tmp" / "integration"
BACKENDS = ("pyspark35", "pyspark40", "spark-connect35", "spark-connect40")
SERVICES = {
    "pyspark35": ("spark35-master", "spark35-worker"),
    "spark-connect35": ("spark35-master", "spark35-worker"),
    "pyspark40": ("spark40-master", "spark40-worker"),
    "spark-connect40": ("spark40-master", "spark40-worker"),
}


def main() -> None:
    args = parse()
    ensure_compose_env()
    if args.down:
        run("down", "--remove-orphans")
        return

    WORKSPACE_TMP.mkdir(parents=True, exist_ok=True)
    backends = BACKENDS if args.backend == "all" else (args.backend,)

    build = ("--build",) if args.build else ()
    run("up", "-d", *build, *_services(backends))
    for backend in backends:
        print(f"\n=== Structure integration: {backend} ===", flush=True)
        run("run", "--rm", f"structure-integration-{backend}")


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Structure integration tests against local Compose infrastructure."
    )
    parser.add_argument("--backend", choices=("all", *BACKENDS), default="all")
    parser.add_argument(
        "--build",
        action="store_true",
        help="Rebuild integration images before running tests.",
    )
    parser.add_argument(
        "--down",
        action="store_true",
        help="Stop the local integration services without deleting their named caches.",
    )
    return parser.parse_args()


def _services(backends: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(service for backend in backends for service in SERVICES[backend]))


def run(*args: str, check: bool = True) -> None:
    command = [
        "docker",
        "compose",
        "--env-file",
        str(ENV),
        "-f",
        str(COMPOSE),
        *args,
    ]
    env = os.environ.copy()
    env["STRUCTURE_ROOT"] = str(ROOT)
    result = subprocess.run(command, cwd=ROOT, env=env)
    if check and result.returncode:
        raise SystemExit(result.returncode)
    if not check and result.returncode:
        print(f"Command exited with {result.returncode}: {' '.join(command)}", file=sys.stderr)


if __name__ == "__main__":
    main()
