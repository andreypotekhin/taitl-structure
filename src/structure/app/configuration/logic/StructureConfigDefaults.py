from pathlib import Path


class StructureConfigDefaults:

    def resolve(self, root: Path) -> tuple[dict[str, object], dict[str, str]]:
        source_roots = ["src"] if (root / "src").exists() else ["."]
        values: dict[str, object] = {
            "source_roots": source_roots,
            "generated_dir": "generated",
            "generated_package": "structure_generated",
            "execution_mode": "online",
            "target_backend": "pyspark",
            "target_pyspark": ">=3.5,<4.1",
            "target_profile": None,
            "compat_targets": [],
            "hook_target_default": ["pyspark"],
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
