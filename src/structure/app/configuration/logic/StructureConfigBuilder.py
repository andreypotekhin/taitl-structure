from collections.abc import Mapping
from pathlib import Path
from typing import cast

from structure.app.configuration.model.StructureConfig import StructureConfig


class StructureConfigBuilder:

    def build(self, root: Path, values: Mapping[str, object], sources: Mapping[str, str]) -> StructureConfig:
        source_roots = cast(list[str], values["source_roots"])
        return StructureConfig(
            project_root=root,
            source_roots=tuple((root / item).resolve() for item in source_roots),
            generated_dir=root / str(values["generated_dir"]),
            generated_package=str(values["generated_package"]),
            execution_mode=str(values["execution_mode"]),
            target_backend=str(values["target_backend"]),
            target_pyspark=str(values["target_pyspark"]),
            traceability=str(values["traceability"]),
            validate_inputs=bool(values["validate_inputs"]),
            input_validation_mode=str(values["input_validation_mode"]),
            validate_intermediate=bool(values["validate_intermediate"]),
            intermediate_validation_mode=str(values["intermediate_validation_mode"]),
            validate_outputs=bool(values["validate_outputs"]),
            output_validation_mode=str(values["output_validation_mode"]),
            strict_performance=bool(values["strict_performance"]),
            fail_on_diff=bool(values["fail_on_diff"]),
            spark_sql={
                "spark.sql.ansi.enabled": values["spark.sql.ansi.enabled"],
                "spark.sql.storeAssignmentPolicy": values["spark.sql.storeAssignmentPolicy"],
            },
            source_map=dict(sources),
        )
