from __future__ import annotations

import re

_CLASSIC_ONLY_ERROR = re.compile(
    r"(spark\s*context|sparkcontext|_jdf|_jvm|_jsc|py4j|java[_\s]gateway|\brdd\b|\.rdd\b|tojavardd)"
)


def is_spark_connect_session(*, session=None, spark=None) -> bool:
    options = getattr(session, "plugin_options", {})
    if options.get("variant") == "spark-connect" or getattr(session, "target_variant", None) == "spark-connect":
        return True

    spark = spark if spark is not None else getattr(session, "spark", None)
    module = type(spark).__module__.lower()
    name = type(spark).__qualname__.lower()
    return "pyspark.sql.connect" in module or ".connect." in module or "connect" in name


def is_classic_only_spark_error(error: Exception) -> bool:
    text = f"{type(error).__module__}.{type(error).__name__}: {error}".lower()
    return bool(_CLASSIC_ONLY_ERROR.search(text)) or ("spark connect" in text and "not supported" in text)
