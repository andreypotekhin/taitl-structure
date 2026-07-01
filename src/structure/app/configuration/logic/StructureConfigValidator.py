from collections.abc import Mapping
from pathlib import Path
from typing import cast

from structure.app.configuration.model.ConfigDiagnostic import ConfigDiagnostic
from structure.app.configuration.model.ConfigError import ConfigError


class StructureConfigValidator:

    _enums = {
        "execution_mode": ("online", "generated"),
        "target_backend": ("pyspark",),
        "traceability": ("none", "compiler", "columns", "debug"),
        "input_validation_mode": ("off", "schema_only", "schema_and_constraints"),
        "intermediate_validation_mode": ("off", "schema_only", "schema_and_constraints"),
        "output_validation_mode": ("off", "schema_only", "schema_and_constraints"),
        "spark.sql.storeAssignmentPolicy": ("ANSI", "LEGACY", "STRICT"),
    }
    _bools = {
        "validate_inputs",
        "validate_intermediate",
        "validate_outputs",
        "strict_performance",
        "fail_on_diff",
        "spark.sql.ansi.enabled",
    }

    def validate(self, values: Mapping[str, object], root: Path) -> None:
        self._validate_type(values["source_roots"], "source_roots", list)
        if not values["source_roots"]:
            self._fail_invalid("source_roots", "source_roots cannot be empty", 'Set source_roots = ["src"].')

        for key in ("generated_dir", "generated_package", "target_pyspark"):
            self._validate_type(values[key], key, str)
        if values["target_profile"] is not None:
            self._validate_type(values["target_profile"], "target_profile", str)
        self._validate_string_list(values["compat_targets"], "compat_targets", "Use compat_targets = [\"polars\"].")
        self._validate_hook_target_default(values["hook_target_default"])
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

    def _fail_invalid(self, setting: str, problem: str, use: str) -> None:
        raise ConfigError(ConfigDiagnostic(code="CONF-E0102", setting=setting, problem=problem, use=use))

    def _inside(self, child: Path, parent: Path) -> bool:
        try:
            child.relative_to(parent)
        except ValueError:
            return False
        return True
