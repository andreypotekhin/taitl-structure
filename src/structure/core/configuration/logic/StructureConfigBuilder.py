from collections.abc import Mapping
from pathlib import Path
from typing import cast

from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration


class StructureConfigBuilder:

    def build(self, root: Path, values: Mapping[str, object], sources: Mapping[str, str]) -> StructureConfig:
        source_roots = cast(list[str], values["source_roots"])
        generated_dir = root / str(values["generated_dir"])
        generated_docs_formats = cast(list[str], values["generated_docs_formats"])
        generated_code_options = cast(list[str], values["generated_code_options"])
        generated_code_hard_wrap = cast(int, values["generated_code_hard_wrap"])
        hook_target_default = values["hook_target_default"]
        plugin_configuration = PluginConfiguration.resolve({"plugin": cast(Mapping[str, object], values["plugin"])})
        plugins = plugin_configuration.plugins
        target = plugin_configuration.default
        if target is None:
            raise ValueError("PLUGIN-E2701: plugin.default must select a plugin.")
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
            generated_docs=bool(values["generated_docs"]),
            generated_docs_dir=generated_dir / str(values["generated_docs_dir"]),
            generated_docs_formats=tuple(generated_docs_formats),
            generated_code_options=tuple(sorted(generated_code_options)),
            generated_code_hard_wrap=generated_code_hard_wrap,
            execution_mode=str(values["execution_mode"]),
            target=target,
            hook_target_default=hook_targets,
            traceability=str(values["traceability"]),
            validate_inputs=bool(values["validate_inputs"]),
            input_validation_mode=str(values["input_validation_mode"]),
            validate_intermediate=bool(values["validate_intermediate"]),
            intermediate_validation_mode=str(values["intermediate_validation_mode"]),
            validate_outputs=bool(values["validate_outputs"]),
            output_validation_mode=str(values["output_validation_mode"]),
            strict_performance=bool(values["strict_performance"]),
            warn_on_udfs=bool(values["warn_on_udfs"]),
            allow_stream_to_batch=bool(values["allow_stream_to_batch"]),
            fail_on_diff=bool(values["fail_on_diff"]),
            spark_sql={
                "spark.sql.ansi.enabled": values["spark.sql.ansi.enabled"],
                "spark.sql.storeAssignmentPolicy": values["spark.sql.storeAssignmentPolicy"],
            },
            plugin_options=plugins,
            source_map=dict(sources),
        )
