import os
from collections.abc import Callable, Iterator
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp
from typing import Any

import pytest
from integration.pyspark.support import backend_matrix
from integration.pyspark.support.backend_matrix import spark
from integration.pyspark.support.compiled_artifacts import CompiledArtifacts
from integration.pyspark.support.generated_sources import GeneratedSources
from integration.pyspark.support.plan_profile import profile_checkpoints, profile_guards, profile_mode
from integration.pyspark.support.rows import clear_rows
from integration.pyspark.support.snapshots import Snapshots
from integration.pyspark.support.timing import phase


@pytest.fixture(autouse=True)
def materialized_rows() -> Iterator[None]:
    clear_rows()
    yield
    clear_rows()


@pytest.fixture
def cache_frames() -> Iterator[Callable[..., None]]:
    cached: list[Any] = []

    def cache(*frames: Any) -> None:
        cached.extend(frame.persist() for frame in frames)

    yield cache

    for frame in reversed(cached):
        frame.unpersist()


@pytest.fixture(scope="module")
def compiled_artifacts() -> Iterator[CompiledArtifacts]:
    owner = CompiledArtifacts()
    try:
        yield owner
    finally:
        owner.close()


@pytest.fixture(scope="module")
def generated_sources(compiled_artifacts) -> GeneratedSources:
    return GeneratedSources(compiled_artifacts)


@pytest.fixture
def runtime_sessions(compiled_artifacts):
    def create(spark_session, **settings):
        return backend_matrix.session(
            spark_session,
            artifacts=compiled_artifacts.pool("runtime"),
            **settings,
        )

    return create


@pytest.fixture
def snapshots(spark, pytestconfig) -> Iterator[Snapshots]:
    shared = Path(pytestconfig.rootpath) / ".pytest-workspace-tmp" / "integration"
    shared.mkdir(parents=True, exist_ok=True)
    root = Path(mkdtemp(prefix="snapshots-", dir=shared))
    try:
        yield Snapshots(spark, root)
    finally:
        with phase("snapshot fixture cleanup"):
            rmtree(root)


__all__ = [
    "cache_frames",
    "compiled_artifacts",
    "generated_sources",
    "runtime_sessions",
    "snapshots",
    "spark",
]


@pytest.fixture(autouse=True)
def query_plan_profile(monkeypatch, request):
    mode = profile_mode(os.environ.get("STRUCTURE_PROFILE_QUERY_PLANS"))
    if mode == "off":
        return
    spark_session = request.getfixturevalue("spark")
    profile_checkpoints(monkeypatch, type(spark_session.range(0)), explain=mode == "explain")
    profile_guards(monkeypatch)
