from typing import Any, cast

from structure.plugin.api.v1 import GenerationAPI, GenerationRequest
from structure.plugin.pyspark.api.PySpark import PySpark


class Generation(GenerationAPI):
    def generate(self, request: GenerationRequest) -> dict[str, str]:
        if request.source_module is None or request.source_schema_modules is None or request.generated_package is None:
            raise ValueError("PySpark generation requires source-module, schema-module, and package context.")
        return PySpark.render.project().source_unit(
            cast(Any, request.payload),
            source_module=request.source_module,
            source_schema_modules=cast(Any, request.source_schema_modules),
            generated_package=request.generated_package,
            semantic_fingerprints=request.semantic_fingerprints,
            generated_code_options=request.generated_code_options,
        )
