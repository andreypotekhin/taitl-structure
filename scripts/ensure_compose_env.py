from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV = ROOT / "infra" / "compose" / ".env"
EXAMPLE = ROOT / "infra" / "compose" / ".env_example"


def main() -> None:
    if ENV.exists():
        print(f"Using existing {ENV.relative_to(ROOT)}")
        return

    if not EXAMPLE.exists():
        raise SystemExit(f"Missing {EXAMPLE.relative_to(ROOT)}; cannot create {ENV.relative_to(ROOT)}.")

    ENV.write_text(EXAMPLE.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"Created {ENV.relative_to(ROOT)} from {EXAMPLE.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
