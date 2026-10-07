from __future__ import annotations

import importlib
import os
import sys
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace as dataclass_replace
from functools import wraps
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from integration.pyspark.support.timing import phase

from structure import *
from structure.core.compiler.artifacts.model import CompiledArtifactPool, CompiledTransform, CompilerOptions
from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.Transform import Transform
from structure.plugin.pyspark import *
from structure.plugin.pyspark import PySpark
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.execution.logic.PlanBoundary import close_plan_boundaries

BACKENDS = ("pyspark35", "pyspark40", "pyspark41", "spark-connect35", "spark-connect40", "spark-connect41")
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
def spark(pytestconfig, monkeypatch):
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

    builder = (
        builder.config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        # Spark's default allows ~2 GiB of diagnostic plan text per query.
        .config("spark.sql.maxPlanStringLength", "8192")
        .config("spark.sql.ui.explainMode", "simple")
    )
    if backend in {"pyspark40", "pyspark41"} and not remote:
        # TransformWithState uses multiple state column families, which the
        # HDFS-backed provider does not support.
        builder = builder.config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
    packages = os.environ.get("STRUCTURE_SPARK_JARS_PACKAGES")
    if packages and not remote:
        builder = builder.config("spark.jars.packages", packages)
    extensions = []
    if packages and "sedona" in packages:
        extensions.append("org.apache.sedona.sql.SedonaSqlExtensions")
    if os.environ.get("STRUCTURE_ICEBERG_TESTS") == "1" and not remote:
        extensions.append("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        shared = Path(pytestconfig.rootpath) / ".pytest-workspace-tmp" / "integration"
        warehouse = shared / "iceberg-warehouse"
        builder = (
            builder.config("spark.sql.catalog.structure_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.structure_iceberg.type", "hadoop")
            .config("spark.sql.catalog.structure_iceberg.warehouse", warehouse.as_uri())
        )
    if extensions and not remote:
        builder = builder.config("spark.sql.extensions", ",".join(extensions))
    if not remote:
        builder = (
            builder.config(
                "spark.sql.artifact.dir",
                os.environ.get("STRUCTURE_SPARK_ARTIFACT_DIR", "/tmp/spark-artifacts"),
            )
            .config("spark.ui.enabled", "false")
            .config("spark.default.parallelism", "2")
        )
        python_path = os.environ.get("PYTHONPATH")
        if python_path:
            builder = builder.config("spark.executorEnv.PYTHONPATH", python_path)

    session = None
    last_error = None
    for _ in range(24):
        try:
            session = builder.getOrCreate()
            if remote:
                _add_connect_python_artifacts(session, Path(pytestconfig.rootpath))
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

    shared = Path(pytestconfig.rootpath) / ".pytest-workspace-tmp" / "integration"
    shared.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="checkpoints-", dir=shared) as checkpoints:
        if not remote:
            session.sparkContext.setCheckpointDir(checkpoints)
        if os.environ.get("STRUCTURE_INTEGRATION_CHECKPOINT_TIMING") == "1":
            frame_type = type(session.range(0))
            checkpoint = frame_type.checkpoint

            @wraps(checkpoint)
            def timed_checkpoint(frame, *args, **kwargs):
                with phase("DataFrame checkpoint (nested in construction)"):
                    return checkpoint(frame, *args, **kwargs)

            monkeypatch.setattr(frame_type, "checkpoint", timed_checkpoint)
        try:
            yield session
        finally:
            with phase("plan boundary cleanup"):
                close_plan_boundaries(session)
            session.stop()
            if hasattr(pyspark, "SparkContext"):
                pyspark.SparkContext._active_spark_context = None


def _add_connect_python_artifacts(session, repository: Path) -> None:
    """Distribute project Python modules to Spark Connect executor workers."""

    with TemporaryDirectory(prefix="structure-connect-python-") as temporary:
        artifact = Path(temporary) / "structure-project.zip"
        with ZipFile(artifact, "w", compression=ZIP_DEFLATED) as archive:
            for package in (repository / "examples", repository / "src" / "structure"):
                for source in package.rglob("*"):
                    if source.is_file():
                        archive.write(source, source.relative_to(repository))
        session.addArtifacts(str(artifact), pyfile=True)


def backend_name() -> str:
    return os.environ.get("STRUCTURE_INTEGRATION_BACKEND", "local")


def target_variant() -> str:
    return "spark-connect" if backend_name().startswith("spark-connect") else "ordinary"


def _target_profile() -> str:
    backend = backend_name()
    if backend.endswith("35"):
        return ">=3.5,<4.0"
    if backend.endswith("40"):
        return ">=4.0,<4.1"
    if backend.endswith("41"):
        return ">=4.1,<4.2"
    return ">=3.5,<4.1"


def _plugin() -> dict[str, dict[str, object]]:
    if "STRUCTURE_CONNECT_PLAN_BOUNDARIES" in os.environ:
        raise ValueError("STRUCTURE_CONNECT_PLAN_BOUNDARIES was removed; use STRUCTURE_PLAN_BOUNDARIES instead.")
    options: dict[str, object] = {"profile": _target_profile(), "variant": target_variant()}
    boundary_policy = os.environ.get("STRUCTURE_PLAN_BOUNDARIES")
    if boundary_policy is not None:
        options["plan_boundaries"] = boundary_policy
    validate_intermediate = os.environ.get("STRUCTURE_VALIDATE_INTERMEDIATE")
    if validate_intermediate is not None:
        options["validate_intermediate"] = validate_intermediate.lower() == "true"
    return {"pyspark": options}


def _prune_unused_steps() -> bool:
    value = os.environ.get("STRUCTURE_PRUNE_UNUSED_STEPS")
    if value is None:
        return True
    if value.lower() not in {"true", "false"}:
        raise ValueError("STRUCTURE_PRUNE_UNUSED_STEPS must be true or false")
    return value.lower() == "true"


def session(
    spark,
    *,
    execution_mode: str,
    generated_package: str | None = None,
    allow_stage_outputs: bool = True,
    artifacts: CompiledArtifactPool | None = None,
) -> StructureSession:
    return StructureSession(
        spark=spark,
        config=StructureConfig.create(
            execution_mode=execution_mode,
            generated_package=generated_package or "structure_generated",
            allow_stage_outputs=allow_stage_outputs,
            prune_unused_steps=_prune_unused_steps(),
            plugin=_plugin(),
        ),
        artifacts=artifacts,
    )


def _compiler_options(
    *, generated_package: str, generated_code_options: tuple[str, ...], allow_stage_outputs: bool
) -> CompilerOptions:
    config = StructureConfig.create(
        execution_mode="generated",
        generated_package=generated_package,
        generated_code_options=generated_code_options,
        allow_stage_outputs=allow_stage_outputs,
        prune_unused_steps=_prune_unused_steps(),
        plugin=_plugin(),
    )
    return CompilerOptions.from_config(config)


def render_generated_project(
    transform_type: type[Transform],
    *,
    source_transform: str,
    generated_package: str,
    source_schema_modules: Mapping[str, Sequence[type[Schema]]],
    generated_code_options: tuple[str, ...] = (),
    allow_stage_outputs: bool = True,
    artifacts: CompiledArtifactPool | None = None,
    spark_sql: Mapping[str, object] | None = None,
) -> dict[str, str]:
    options = _compiler_options(
        generated_package=generated_package,
        generated_code_options=generated_code_options,
        allow_stage_outputs=allow_stage_outputs,
    )
    if spark_sql:
        options = dataclass_replace(options, spark_sql={**options.spark_sql, **spark_sql})
    artifact = cast(
        CompiledTransform,
        (artifacts or CompiledArtifactPool()).get_or_compile(transform_type, options=options),
    )
    plan = cast(PySparkExecutionPlan, artifact.pyspark_plan)
    return PySpark.render.project()(
        plan,
        source_transform=source_transform,
        generated_package=generated_package,
        source_schema_modules=source_schema_modules,
        semantic_fingerprint=artifact.semantic_fingerprint,
        generated_code_options=generated_code_options,
    )


def render_generated_projects(
    transforms: Sequence[tuple[type[Transform], str]],
    *,
    generated_package: str,
    source_schema_modules: Mapping[str, Sequence[type[Schema]]],
    generated_code_options: tuple[str, ...] = (),
    allow_stage_outputs: bool = True,
    artifacts: CompiledArtifactPool | None = None,
) -> dict[str, str]:
    options = _compiler_options(
        generated_package=generated_package,
        generated_code_options=generated_code_options,
        allow_stage_outputs=allow_stage_outputs,
    )
    plans_by_module: dict[str, dict[str, PySparkExecutionPlan]] = {}
    fingerprints_by_module: dict[str, dict[str, str]] = {}
    for transform_type, source_transform in transforms:
        artifact = cast(
            CompiledTransform,
            (artifacts or CompiledArtifactPool()).get_or_compile(transform_type, options=options),
        )
        plan = cast(PySparkExecutionPlan, artifact.pyspark_plan)
        source_module = source_transform.rsplit(".", 1)[0]
        plans_by_module.setdefault(source_module, {})[source_transform] = plan
        fingerprints_by_module.setdefault(source_module, {})[source_transform] = artifact.semantic_fingerprint

    files: dict[str, str] = {}
    for source_module, plans in plans_by_module.items():
        files.update(
            PySpark.render.project().source_unit(
                plans,
                source_module=source_module,
                source_schema_modules=source_schema_modules,
                generated_package=generated_package,
                semantic_fingerprints=fingerprints_by_module[source_module],
                generated_code_options=generated_code_options,
            )
        )
    return files


@contextmanager
def generated_project(tmp_path: Path, package: str, files: dict[str, str]) -> Iterator[None]:
    with phase(f"generated source setup ({package})"):
        write_files(tmp_path, files)
    sys.path.insert(0, str(tmp_path))
    try:
        importlib.invalidate_caches()
        yield
    finally:
        with phase(f"generated source cleanup ({package})"):
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
    checked = {path: source for path, source in files.items() if "/pyspark/transforms/" in path or "/runtime/" in path}
    for path, source in checked.items():
        for token in CLASSIC_ONLY_TOKENS:
            assert token not in source, f"{path} contains Spark classic-only token {token!r}"
