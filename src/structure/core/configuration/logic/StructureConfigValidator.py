from collections.abc import Mapping
from pathlib import Path
from typing import cast

from structure.core.configuration.model.ConfigDiagnostic import ConfigDiagnostic
from structure.core.configuration.model.ConfigError import ConfigError
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration


class StructureConfigValidator:

    _generated_code_options = {"mirror_methods", "embed_exprs", "embed_hooks", "embed_udfs"}

    _enums = {
        "execution_mode": ("online", "generated"),
        "traceability": ("none", "compiler", "columns", "debug"),
        "input_validation_mode": ("off", "schema_only", "schema_and_constraints"),
        "intermediate_validation_mode": ("off", "schema_only", "schema_and_constraints"),
        "output_validation_mode": ("off", "schema_only", "schema_and_constraints"),
        "stream_to_batch_policy": ("default", "strict"),
        "spark.sql.storeAssignmentPolicy": ("ANSI", "LEGACY", "STRICT"),
    }
    _bools = {
        "generated_docs",
        "validate_inputs",
        "validate_intermediate",
        "validate_outputs",
        "strict_performance",
        "warn_on_udfs",
        "warn_on_lineage_growth",
        "allow_stream_to_batch",
        "allow_output_to_input",
        "allow_to_reassign_output",
        "allow_stage_outputs",
        "fail_on_diff",
        "spark.sql.ansi.enabled",
    }

    def validate(self, values: Mapping[str, object], root: Path, *, allow_empty_source_roots: bool = False) -> None:
        self._validate_type(values["source_roots"], "source_roots", list)
        if not values["source_roots"] and not allow_empty_source_roots:
            self._fail_invalid("source_roots", "source_roots cannot be empty", 'Set source_roots = ["src"].')

        for key in ("generated_dir", "generated_package", "generated_docs_dir"):
            self._validate_type(values[key], key, str)
        self._validate_string_list(
            values["generated_docs_formats"],
            "generated_docs_formats",
            'Use generated_docs_formats = ["markdown", "json"].',
        )
        self._validate_generated_code_options(values["generated_code_options"])
        self._validate_generated_code_hard_wrap(values["generated_code_hard_wrap"])
        self._validate_hook_target_default(values["hook_target_default"])
        self._validate_plugin_options(values["plugin"])
        for key in self._bools:
            self._validate_type(values[key], key, bool)
        for key, allowed in self._enums.items():
            if values[key] not in allowed:
                self._fail_invalid(key, f"Invalid value {values[key]!r}", f"Use one of: {', '.join(allowed)}.")

        package = str(values["generated_package"])
        if package == "structure" or not all(part.isidentifier() for part in package.split(".")):
            self._fail_invalid(
                "generated_package",
                "generated_package must be a non-structure dotted package name",
                'Use "structure_generated".',
            )

        generated_dir_value = Path(str(values["generated_dir"]))
        if generated_dir_value.is_absolute():
            self._fail_invalid("generated_dir", "generated_dir must be project-relative in v1", 'Use "generated".')
        generated_dir = root / generated_dir_value
        generated_docs_dir = Path(str(values["generated_docs_dir"]))
        if generated_docs_dir.is_absolute() or ".." in generated_docs_dir.parts:
            self._fail_invalid(
                "generated_docs_dir",
                "generated_docs_dir must stay inside generated_dir",
                'Use "docs".',
            )

        formats = cast(list[str], values["generated_docs_formats"])
        allowed_formats = {"markdown", "json"}
        invalid_formats = sorted(set(formats) - allowed_formats)
        if invalid_formats:
            self._fail_invalid(
                "generated_docs_formats",
                f"Unsupported docs format: {', '.join(invalid_formats)}",
                "Use markdown, json, or both.",
            )

        source_roots = cast(list[str], values["source_roots"])
        for item in source_roots:
            if not isinstance(item, str):
                self._fail_invalid(
                    "source_roots", "source_roots must be a list of strings", 'Use source_roots = ["src"].'
                )
            source_root = root / item
            if not source_root.exists():
                self._fail_invalid(
                    "source_roots",
                    f"Source root does not exist: {item}",
                    "Create the directory or adjust source_roots.",
                )
            if self._inside(source_root.resolve(), generated_dir.resolve()):
                self._fail_invalid(
                    "source_roots",
                    "source_roots must not be inside generated_dir",
                    "Move generated output outside source_roots.",
                )

    def _validate_type(self, value: object, key: str, type_: type) -> None:
        if not isinstance(value, type_):
            self._fail_invalid(
                key, f"Expected {type_.__name__}, got {type(value).__name__}", f"Set {key} to a valid {type_.__name__}."
            )

    def _validate_string_list(self, value: object, key: str, use: str) -> None:
        self._validate_type(value, key, list)
        if not all(isinstance(item, str) and item for item in cast(list[object], value)):
            self._fail_invalid(key, f"{key} must be a list of non-empty strings", use)

    def _validate_hook_target_default(self, value: object) -> None:
        if value == "explicit":
            return
        if isinstance(value, str):
            self._fail_invalid(
                "hook_target_default",
                "hook_target_default must be a list of backend names or explicit",
                'Use hook_target_default = ["pyspark"].',
            )
        self._validate_string_list(
            value,
            "hook_target_default",
            'Use hook_target_default = ["pyspark"] or hook_target_default = "explicit".',
        )

    def _validate_generated_code_options(self, value: object) -> None:
        if not isinstance(value, (list, tuple)):
            self._fail_invalid(
                "generated_code_options",
                f"Expected list, got {type(value).__name__}",
                'Use generated_code_options = ["mirror_methods", "embed_exprs"].',
            )
        options = cast(tuple[str, ...] | list[str], value)
        if not all(isinstance(item, str) and item for item in options):
            self._fail_invalid(
                "generated_code_options",
                "generated_code_options must contain non-empty strings",
                'Use generated_code_options = ["mirror_methods", "embed_exprs"].',
            )
        if len(options) != len(set(options)):
            self._fail_invalid(
                "generated_code_options",
                "generated_code_options must not contain duplicates",
                'Use generated_code_options = ["mirror_methods", "embed_exprs"].',
            )
        unknown = sorted(set(options) - self._generated_code_options)
        if unknown:
            self._fail_invalid(
                "generated_code_options",
                f"Unsupported generated code option(s): {', '.join(unknown)}",
                'Use mirror_methods, embed_exprs, embed_hooks, or embed_udfs.',
            )

    def _validate_generated_code_hard_wrap(self, value: object) -> None:
        if not isinstance(value, int) or isinstance(value, bool):
            self._fail_invalid(
                "generated_code_hard_wrap",
                f"Expected int, got {type(value).__name__}",
                "Use generated_code_hard_wrap = 120.",
            )
        hard_wrap = cast(int, value)
        if hard_wrap < 80:
            self._fail_invalid(
                "generated_code_hard_wrap",
                "generated_code_hard_wrap must be at least 80",
                "Use generated_code_hard_wrap = 120.",
            )

    def _validate_plugin_options(self, value: object) -> None:
        if not isinstance(value, Mapping):
            self._fail_invalid("plugin", "plugin must be a table", 'Use [plugin.pyspark].')
        try:
            PluginConfiguration.resolve({"plugin": cast(Mapping[str, object], value)})
        except ValueError as error:
            self._fail_invalid("plugin", str(error), 'Use [plugin] and [plugin.pyspark].')

    def _fail_invalid(self, setting: str, problem: str, use: str) -> None:
        raise ConfigError(ConfigDiagnostic(code="CONF-E0102", setting=setting, problem=problem, use=use))

    def _inside(self, child: Path, parent: Path) -> bool:
        try:
            child.relative_to(parent)
        except ValueError:
            return False
        return True
