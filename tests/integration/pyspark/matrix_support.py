from __future__ import annotations

import importlib
import os
import sys
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path

import pytest

from structure import StructureSession
from structure.app.dsl.api import compile_transform
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.target.capabilities.api import Capabilities
from structure.app.target.pyspark.api import PySpark

BACKENDS = ("pyspark35", "pyspark40", "spark-connect35", "spark-connect40")
CLASSIC_ONLY_TOKENS = (
    "SparkContext",
    "sparkContext",
    "SQLContext",
    "sql_ctx",
    "_jdf",
    "_jvm",
    ".rdd",
    ".collect(",
    ".toPandas(",
    "foreachPartition",
    "mapInPandas",
)


@pytest.fixture
def spark():
    pyspark = pytest.importorskip("pyspark")
    sql = pytest.importorskip("pyspark.sql")
    backend = backend_name()
    remote = os.environ.get("STRUCTURE_SPARK_REMOTE")
    master = os.environ.get("STRUCTURE_SPARK_MASTER", "local[2]")
    builder = sql.SparkSession.builder.appName(f"structure-integration-{backend}")
    if remote:
        builder = builder.remote(remote)
    else:
        builder = builder.master(master)

    builder = builder.config("spark.sql.shuffle.partitions", "1").config("spark.sql.session.timeZone", "UTC")
    if not remote:
        builder = builder.config("spark.sql.artifact.dir", "/tmp/spark-artifacts").config("spark.ui.enabled", "false")

    session = None
    last_error = None
    for _ in range(24):
        try:
            session = builder.getOrCreate()
            session.range(1).count()
            break
        except Exception as error:  # pragma: no cover - only exercised while Spark starts.
            last_error = error
            if session is not None:
                session.stop()
            time.sleep(2)

    if session is None:
        endpoint = remote or master
        raise AssertionError(f"Spark did not become ready at {endpoint}: {last_error}")

    try:
        yield session
    finally:
        session.stop()
        if hasattr(pyspark, "SparkContext"):
            pyspark.SparkContext._active_spark_context = None


def backend_name() -> str:
    return os.environ.get("STRUCTURE_INTEGRATION_BACKEND", "local")


def target_variant() -> str:
    return "spark-connect" if backend_name().startswith("spark-connect") else "ordinary"


def session(spark, *, execution_mode: str, generated_package: str | None = None) -> StructureSession:
    return StructureSession(
        spark=spark,
        execution_mode=execution_mode,
        generated_package=generated_package,
        target_variant=target_variant(),
    )


def render_generated_project(
    transform_type: type[Transform],
    *,
    source_transform: str,
    generated_package: str,
    source_schema_modules: Mapping[str, Sequence[type[Structure]]],
) -> dict[str, str]:
    capabilities = Capabilities.resolve()(target_backend="pyspark", target_variant=target_variant())
    return PySpark.render.project()(
        PySpark.plan.lower()(compile_transform(transform_type), capabilities=capabilities),
        source_transform=source_transform,
        generated_package=generated_package,
        source_schema_modules=source_schema_modules,
    )


@contextmanager
def generated_project(tmp_path: Path, package: str, files: dict[str, str]) -> Iterator[None]:
    write_files(tmp_path, files)
    sys.path.insert(0, str(tmp_path))
    try:
        importlib.invalidate_caches()
        yield
    finally:
        sys.path.remove(str(tmp_path))
        drop_generated_modules(package)


def write_files(root: Path, files: dict[str, str]) -> None:
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def drop_generated_modules(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            sys.modules.pop(name, None)


def assert_generated_connect_safe(files: Mapping[str, str]) -> None:
    checked = {
        path: source
        for path, source in files.items()
        if "/pyspark/transforms/" in path or "/runtime/" in path
    }
    for path, source in checked.items():
        for token in CLASSIC_ONLY_TOKENS:
            assert token not in source, f"{path} contains Spark classic-only token {token!r}"
