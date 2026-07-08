from collections.abc import Mapping
from pathlib import Path
from typing import cast

from structure.app.configuration.model.StructureConfig import StructureConfig


class StructureConfigBuilder:

    def build(self, root: Path, values: Mapping[str, object], sources: Mapping[str, str]) -> StructureConfig:
        source_roots = cast(list[str], values["source_roots"])
        compat_targets = cast(list[str], values["compat_targets"])
        generated_dir = root / str(values["generated_dir"])
        generated_docs_formats = cast(list[str], values["generated_docs_formats"])
        hook_target_default = values["hook_target_default"]
        hook_targets = (
            str(hook_target_default)
            if isinstance(hook_target_default, str)
            else tuple(cast(list[str], hook_target_default))
        )
        return StructureConfig(
            project_root=root,
            source_roots=tuple((root / item).resolve() for item in source_roots),
            generated_dir=generated_dir,
            generated_package=str(values["generated_package"]),
            generated_docs_dir=generated_dir / str(values["generated_docs_dir"]),
            generated_docs_formats=tuple(generated_docs_formats),
            execution_mode=str(values["execution_mode"]),
            target_backend=str(values["target_backend"]),
            target_profile=str(values["target_profile"]),
            target_variant=str(values["target_variant"]),
            compat_targets=tuple(compat_targets),
            hook_target_default=hook_targets,
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
