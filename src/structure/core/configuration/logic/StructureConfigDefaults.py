from pathlib import Path


class StructureConfigDefaults:

    def programmatic(self) -> tuple[dict[str, object], dict[str, str]]:
        values, sources = self.resolve_values(source_roots=[])
        return values, sources

    def resolve(self, root: Path) -> tuple[dict[str, object], dict[str, str]]:
        source_roots = ["src"] if (root / "src").exists() else ["."]
        return self.resolve_values(source_roots=source_roots)

    @staticmethod
    def resolve_values(*, source_roots: list[str]) -> tuple[dict[str, object], dict[str, str]]:
        values: dict[str, object] = {
            "source_roots": source_roots,
            "generated_dir": "generated",
            "generated_package": "structure_generated",
            "generated_docs": True,
            "generated_docs_dir": "docs",
            "generated_docs_formats": ["markdown", "json"],
            "generated_code_options": [],
            "generated_code_hard_wrap": 120,
            "execution_mode": "online",
            "hook_target_default": ["pyspark"],
            "traceability": "compiler",
            "validate_inputs": True,
            "input_validation_mode": "schema_only",
            "validate_intermediate": True,
            "intermediate_validation_mode": "schema_only",
            "validate_outputs": True,
            "output_validation_mode": "schema_only",
            "strict_performance": True,
            "warn_on_udfs": True,
            "allow_stream_to_batch": False,
            "stream_to_batch_policy": "default",
            "fail_on_diff": False,
            "spark.sql.ansi.enabled": True,
            "spark.sql.storeAssignmentPolicy": "ANSI",
            "plugin": {"default": "pyspark", "pyspark": {"profile": ">=3.5,<4.1", "variant": "ordinary"}},
        }
        return values, {key: "default" for key in values}
