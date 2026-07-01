from __future__ import annotations

from pathlib import Path

import click


class WriteStructureConfig:

    def __call__(self, *, root: Path, seed_config: bool) -> tuple[str, ...]:
        pyproject = root / "pyproject.toml"
        structure = root / "structure.toml"
        if pyproject.exists():
            text = pyproject.read_text(encoding="utf-8")
            if "[tool.structure]" in text:
                raise click.ClickException("Structure configuration already exists in pyproject.toml")
            pyproject.write_text(text.rstrip() + "\n\n" + self._config(seed_config), encoding="utf-8")
            return ("Wrote pyproject.toml [tool.structure]",)
        if structure.exists():
            raise click.ClickException("Structure configuration already exists in structure.toml")
        structure.write_text(self._config(seed_config), encoding="utf-8")
        return ("Wrote structure.toml",)

    def _config(self, seed: bool) -> str:
        lines = [
            "[tool.structure]",
            'source_roots = ["src"]',
            'generated_dir = "generated"',
            'generated_package = "structure_generated"',
            'execution_mode = "online"',
            'target_backend = "pyspark"',
            'target_pyspark = ">=3.5,<4.1"',
            'hook_target_default = ["pyspark"]',
            'traceability = "compiler"',
        ]
        if seed:
            lines.extend(
                [
                    "validate_inputs = true",
                    'input_validation_mode = "schema_only"',
                    "validate_intermediate = true",
                    'intermediate_validation_mode = "schema_only"',
                    "validate_outputs = true",
                    'output_validation_mode = "schema_only"',
                    "strict_performance = true",
                    "fail_on_diff = false",
                ]
            )
        return "\n".join(lines) + "\n"
