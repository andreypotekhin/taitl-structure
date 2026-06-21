from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class StructureConfig:
    project_root: Path
    source_roots: tuple[Path, ...]
    generated_dir: Path
    generated_package: str
    execution_mode: str
    target_backend: str
    target_pyspark: str
    lineage: str
    validate_inputs: bool
    input_validation_mode: str
    validate_intermediate: bool
    intermediate_validation_mode: str
    validate_outputs: bool
    output_validation_mode: str
    strict_performance: bool
    fail_on_diff: bool
    spark_sql: Mapping[str, object]
    source_map: Mapping[str, str]
