import re
from pathlib import Path

API_ROOT = Path("src/structure/app")
LOWERCASE_ENDPOINTS = {
    "capabilities",
    "cli_actions",
    "compiler",
    "configuration",
    "execution",
    "frontend",
    "generated",
    "online",
    "pyspark",
    "runtime",
    "schemas",
}


def test_public_app_apis_do_not_export_lowercase_endpoint_instances() -> None:
    for file in API_ROOT.glob("**/api/__init__.py"):
        text = file.read_text()
        for name in LOWERCASE_ENDPOINTS:
            assert not re.search(rf"(?m)^{name}\s*=", text)
            assert f'"{name}"' not in text


def test_public_app_apis_do_not_import_public_commands_or_models_from_logic() -> None:
    for file in API_ROOT.glob("**/api/*.py"):
        text = file.read_text()
        assert ".logic.actions" not in text
        assert ".logic.model" not in text
