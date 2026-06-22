from __future__ import annotations

import pytest


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run live integration tests that may start or contact backend infrastructure.",
    )
    parser.addoption(
        "--integration-backend",
        action="store",
        choices=("all", "pyspark35", "pyspark40"),
        default="all",
        help="Select the integration backend lane to exercise.",
    )


def pytest_collection_modifyitems(config, items) -> None:
    if config.getoption("--run-integration"):
        return

    skip = pytest.mark.skip(reason="integration test requires --run-integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
