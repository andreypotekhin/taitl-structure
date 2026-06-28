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
BACKENDS = ("pyspark35", "pyspark40")
SERVICES = ("spark35-master", "spark35-worker", "spark40-master", "spark40-worker")


def main() -> None:
    args = parse()
    ensure_compose_env()
    backends = BACKENDS if args.backend == "all" else (args.backend,)

    try:
        run("up", "-d", "--build", *SERVICES)
        for backend in backends:
            run("run", "--rm", f"structure-integration-{backend}")
    finally:
        run("down", "--remove-orphans", check=False)


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Structure integration tests against local Compose infrastructure."
    )
    parser.add_argument("--backend", choices=("all", *BACKENDS), default="all")
    return parser.parse_args()


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
