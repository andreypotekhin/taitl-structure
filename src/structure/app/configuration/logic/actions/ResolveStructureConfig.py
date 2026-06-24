from __future__ import annotations

import difflib
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import toml  # type: ignore[import-untyped]

from structure.app.configuration.logic.model.ConfigDiagnostic import ConfigDiagnostic
from structure.app.configuration.logic.model.ConfigError import ConfigError
from structure.app.configuration.logic.model.StructureConfig import StructureConfig
from structure.app.target.capabilities.api import capabilities


class ResolveStructureConfig:

    _keys = {
        "source_roots",
        "generated_dir",
        "generated_package",
        "execution_mode",
        "target_backend",
        "target_pyspark",
        "traceability",
        "validate_inputs",
        "input_validation_mode",
        "validate_intermediate",
        "intermediate_validation_mode",
        "validate_outputs",
        "output_validation_mode",
        "strict_performance",
        "fail_on_diff",
        "spark.sql.ansi.enabled",
        "spark.sql.storeAssignmentPolicy",
    }
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

    def __call__(
        self,
        *,
        project_root: Path | str | None = None,
        overrides: Mapping[str, object] | None = None,
    ) -> StructureConfig:
        root = Path(project_root or Path.cwd()).resolve()
        values, sources = self._defaults(root)
        self._merge(values, sources, self._load_structure_toml(root), "structure.toml")
        self._merge(values, sources, self._load_pyproject(root), "pyproject.toml [tool.structure]")
        self._merge(
            values, sources, {key: value for key, value in (overrides or {}).items() if value is not None}, "CLI"
        )
        self._validate(values, root)
        capabilities.resolve()(
            target_backend=str(values["target_backend"]),
            target_pyspark=str(values["target_pyspark"]),
        )
        return self._config(root, values, sources)

    def _defaults(self, root: Path) -> tuple[dict[str, object], dict[str, str]]:
        source_roots = ["src"] if (root / "src").exists() else ["."]
        values: dict[str, object] = {
            "source_roots": source_roots,
            "generated_dir": "generated",
            "generated_package": "structure_generated",
            "execution_mode": "online",
            "target_backend": "pyspark",
            "target_pyspark": ">=3.5,<4.1",
            "traceability": "compiler",
            "validate_inputs": True,
            "input_validation_mode": "schema_only",
            "validate_intermediate": True,
            "intermediate_validation_mode": "schema_only",
            "validate_outputs": True,
            "output_validation_mode": "schema_only",
            "strict_performance": True,
            "fail_on_diff": False,
            "spark.sql.ansi.enabled": True,
            "spark.sql.storeAssignmentPolicy": "ANSI",
        }
        return values, {key: "default" for key in values}

    def _load_structure_toml(self, root: Path) -> dict[str, object]:
        path = root / "structure.toml"
        if not path.exists():
            return {}
        data = toml.load(path)
        return self._section(data)

    def _load_pyproject(self, root: Path) -> dict[str, object]:
        path = root / "pyproject.toml"
        if not path.exists():
            return {}
        data = toml.load(path)
        tool = data.get("tool", {})
        if not isinstance(tool, dict):
            return {}
        structure = tool.get("structure", {})
        return self._flatten(structure) if isinstance(structure, dict) else {}

    def _section(self, data: Mapping[str, object]) -> dict[str, object]:
        tool = data.get("tool")
        if isinstance(tool, dict) and isinstance(tool.get("structure"), dict):
            return self._flatten(tool["structure"])
        return self._flatten(data)

    def _flatten(self, data: Mapping[str, object], prefix: str = "") -> dict[str, object]:
        flat: dict[str, object] = {}
        for key, value in data.items():
            name = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, dict):
                flat.update(self._flatten(value, name))
            else:
                flat[name] = value
        return flat

    def _merge(
        self, values: dict[str, object], sources: dict[str, str], incoming: Mapping[str, object], source: str
    ) -> None:
        for key, value in incoming.items():
            if key not in self._keys:
                self._fail_unknown(key)
            values[key] = value
            sources[key] = source

    def _validate(self, values: Mapping[str, object], root: Path) -> None:
        self._validate_type(values["source_roots"], "source_roots", list)
        if not values["source_roots"]:
            self._fail_invalid("source_roots", "source_roots cannot be empty", 'Set source_roots = ["src"].')

        for key in ("generated_dir", "generated_package", "target_pyspark"):
            self._validate_type(values[key], key, str)
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

    def _config(self, root: Path, values: Mapping[str, object], sources: Mapping[str, str]) -> StructureConfig:
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

    def _validate_type(self, value: object, key: str, type_: type) -> None:
        if not isinstance(value, type_):
            self._fail_invalid(
                key, f"Expected {type_.__name__}, got {type(value).__name__}", f"Set {key} to a valid {type_.__name__}."
            )

    def _fail_unknown(self, key: str) -> None:
        suggestion = difflib.get_close_matches(key, self._keys, n=1)
        use = (
            f"Did you mean {suggestion[0]}?"
            if suggestion
            else "Remove the key or add it to the config specification first."
        )
        raise ConfigError(
            ConfigDiagnostic(code="CONF-E0101", setting=key, problem="Unknown configuration key", use=use)
        )

    def _fail_invalid(self, setting: str, problem: str, use: str) -> None:
        raise ConfigError(ConfigDiagnostic(code="CONF-E0102", setting=setting, problem=problem, use=use))

    def _inside(self, child: Path, parent: Path) -> bool:
        try:
            child.relative_to(parent)
        except ValueError:
            return False
        return True


resolve_structure_config = ResolveStructureConfig()
