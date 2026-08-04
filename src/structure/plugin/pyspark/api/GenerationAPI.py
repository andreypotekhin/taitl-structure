from typing import Any, cast

from structure.plugin.api.v1 import GenerationAPI as GenerationAPIV1
from structure.plugin.api.v1 import GenerationRequest, GenerationResult
from structure.plugin.pyspark.api.PySpark import PySpark
from structure.plugin.pyspark.GeneratedPySparkTransformModule import generated_pyspark_transform_module


class GenerationAPI(GenerationAPIV1):
    def generate(self, request: GenerationRequest) -> GenerationResult:
        if request.source_module is None or request.source_schema_modules is None or request.generated_package is None:
            raise ValueError("PySpark generation requires source-module, schema-module, and package context.")
        files = PySpark.render.project().source_unit(
            cast(Any, request.payload),
            source_module=request.source_module,
            source_schema_modules=cast(Any, request.source_schema_modules),
            generated_package=request.generated_package,
            semantic_fingerprints=request.semantic_fingerprints,
            generated_code_options=request.generated_code_options,
            generated_code_hard_wrap=request.generated_code_hard_wrap,
        )
        source = request.source_module
        plans = cast(dict[str, object], request.payload)
        return GenerationResult(
            files=files,
            module_name=generated_pyspark_transform_module(source, generated_package=request.generated_package),
            classes=tuple(f"{name.rsplit('.', 1)[1]}Generated" for name in plans),
        )
