from __future__ import annotations

from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.runtime.session.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.core.runtime.session.model.StructureRuntimeError import StructureRuntimeError
from structure.platform.pyspark.logic.SparkConnectCompatibility import (
    is_classic_only_spark_error,
    is_spark_connect_session,
)


def spark_connect_runtime_error(
    invocation: Transform,
    *,
    session,
    error: Exception,
    surface: str,
) -> StructureRuntimeError | None:
    if not is_spark_connect_session(session=session) or not is_classic_only_spark_error(error):
        return None

    transform = f"{type(invocation).__module__}.{type(invocation).__name__}"
    diagnostic = RuntimeDiagnostic(
        code="CONNECT-E2601",
        title="Spark Connect boundary is unsupported",
        transform=transform,
        execution_mode=session.execution_mode,
        target_backend=session.target_backend,
        target_profile=getattr(session, "target_profile", ">=3.5,<4.1"),
        target_variant=getattr(session, "target_variant", "spark-connect"),
        problem=(
            f"{surface} used classic-only Spark internals while running with Spark Connect. "
            "Spark Connect cannot access SparkContext, JVM handles, RDD APIs, or Py4J gateway objects."
        ),
        use=(
            "Remove classic-only Spark/Py4J/RDD access, use Spark Connect DataFrame APIs, "
            "or run with target_variant = \"ordinary\" when the code requires classic PySpark internals."
        ),
        docs="docs/reference/SparkConnect.md",
        context={"surface": surface, "cause": type(error).__name__},
    )
    return StructureRuntimeError(diagnostic)
