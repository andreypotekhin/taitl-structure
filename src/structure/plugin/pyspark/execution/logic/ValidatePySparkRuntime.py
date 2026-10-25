from __future__ import annotations

import logging
import re
import weakref
from typing import Any

from structure.plugin.api.v1.model import RuntimeDiagnostic, StructureRuntimeError
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PROFILE_VERSION_BOUNDS

LOGGER = logging.getLogger("structure.plugin.pyspark.runtime")
_VERSION = re.compile(r"^(\d+)\.(\d+)(?:\.(\d+))?(?=$|[^0-9])")
_SUCCESSFUL: weakref.WeakKeyDictionary[object, set[tuple[str, str, str, str]]] = weakref.WeakKeyDictionary()
_LOGGED: weakref.WeakKeyDictionary[object, set[tuple[str, str, str, str]]] = weakref.WeakKeyDictionary()


def validate_pyspark_runtime(session: Any, *, transform: str = "", execution_mode: str = "online") -> None:
    """Check the configured profile against installed client and supplied engine versions."""
    options = getattr(session, "plugin_options", {})
    if not isinstance(options, dict) and not hasattr(options, "get"):
        options = {}
    profile = str(options.get("profile", ">=3.5,<4.2"))
    variant = str(options.get("variant", "ordinary"))
    policy = options.get("runtime_version_check", "raise")
    if not isinstance(policy, str) or policy not in {"raise", "log", "off"}:
        raise ValueError("PySpark runtime_version_check must be 'raise', 'log', or 'off'.")
    if profile not in PROFILE_VERSION_BOUNDS:
        raise ValueError(f"Unsupported PySpark profile {profile!r}; choose a supported profile.")
    if policy == "off":
        return

    runtime = getattr(session, "runtime", None)
    if runtime is None:
        return
    try:
        cached = _SUCCESSFUL.get(runtime)
    except TypeError:
        cached = None
    key = (profile, variant, policy, execution_mode)
    if cached is not None and key in cached:
        return
    if policy == "log":
        try:
            if key in _LOGGED.get(runtime, set()):
                return
        except TypeError:
            pass

    client_version: object = None
    engine_version: object = None
    failure_source = ""
    try:
        import pyspark

        client_version = pyspark.__version__
    except Exception:
        failure_source = "PySpark client (pyspark.__version__)"
    if not failure_source:
        try:
            engine_version = runtime.version
        except Exception:
            failure_source = "Spark engine (session.version)"

    bounds = PROFILE_VERSION_BOUNDS[profile]
    parsed_client = _parse(client_version)
    parsed_engine = _parse(engine_version)
    if not failure_source and parsed_client is None:
        failure_source = "PySpark client (pyspark.__version__)"
    if not failure_source and parsed_engine is None:
        failure_source = "Spark engine (session.version)"

    outside = []
    if not failure_source and parsed_client is not None and parsed_engine is not None:
        if not bounds[0] <= parsed_client < bounds[1]:
            outside.append("client")
        if not bounds[0] <= parsed_engine < bounds[1]:
            outside.append("engine")
    if not failure_source and not outside:
        try:
            _SUCCESSFUL.setdefault(runtime, set()).add(key)
        except TypeError:
            pass
        return

    code = "PYSPARK-E2711" if failure_source else "PYSPARK-E2710"
    reason = (
        f"Could not read or parse the {failure_source}; version must be numeric major.minor[.patch]."
        if failure_source
        else f"The {' and '.join(outside)} version is outside profile {profile!r}."
    )
    problem = (
        f"Configured profile: {profile}; variant: {variant}; client: {client_version!r}; "
        f"engine: {engine_version!r}; failing component: {failure_source or ', '.join(outside)}."
    )
    use = (
        "Set [tool.structure.plugin.pyspark] profile to a supported range containing both versions, or install/use "
        "a matching PySpark client and Spark engine. Deliberately set runtime_version_check = 'log' to continue "
        "with an ERROR log, or 'off' to skip version discovery. Profile selection does not enable version-specific "
        "capabilities. See docs/reference/Configuration.ref.md and Troubleshooting.md."
    )
    diagnostic = RuntimeDiagnostic(
        code=code,
        title="Spark runtime profile check failed" if code.endswith("2710") else "Spark runtime version could not be read",
        transform=transform,
        execution_mode=execution_mode,
        target=profile,
        problem=problem,
        use=use,
        docs="docs/Diagnostics.md#" + code.lower(),
        context={
            "target_variant": variant,
            "client_version": str(client_version),
            "engine_version": str(engine_version),
            "failing_component": failure_source or ", ".join(outside),
            "reason": reason,
            "runtime_version_check": str(policy),
        },
    )
    if policy == "log":
        LOGGER.error("%s", diagnostic.render())
        if not failure_source:
            try:
                _LOGGED.setdefault(runtime, set()).add(key)
            except TypeError:
                pass
        return
    raise StructureRuntimeError(diagnostic)


def _parse(value: object) -> tuple[int, int, int] | None:
    if not isinstance(value, str):
        return None
    match = _VERSION.match(value.strip())
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2)), int(match.group(3) or 0)


validate_pyspark_runtime_guard = validate_pyspark_runtime


def validate_generated_pyspark_runtime(
    spark: object,
    *,
    profile: str,
    variant: str,
    policy: str,
    transform: str,
) -> None:
    """Apply the shared guard to a directly instantiated generated transform."""
    from types import SimpleNamespace

    session = SimpleNamespace(
        runtime=spark,
        plugin_options={"profile": profile, "variant": variant, "runtime_version_check": policy},
    )
    validate_pyspark_runtime(session, transform=transform, execution_mode="generated")
