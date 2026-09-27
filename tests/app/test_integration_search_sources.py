import ast
import importlib
import sys
from types import ModuleType

from helpers import fake_pyspark_schema as FakeTypes
from integration.pyspark.search.test_search import PACKAGE, SCHEMA_MODULES, ExactSimilarityCandidates
from integration.pyspark.support.backend_matrix import generated_project

from structure.plugin.pyspark import PySpark


def test_search_similarity_fixture_supplies_importable_generated_schemas(tmp_path, monkeypatch) -> None:
    artifact = ExactSimilarityCandidates.compile(schema_types=FakeTypes, allow_stage_outputs=False)
    source = f"{ExactSimilarityCandidates.__module__}.{ExactSimilarityCandidates.__name__}"
    files = PySpark.render.project()(
        artifact.pyspark_plan,
        source_transform=source,
        generated_package=PACKAGE,
        source_schema_modules=SCHEMA_MODULES,
        semantic_fingerprint=artifact.semantic_fingerprint,
    )
    pyspark = ModuleType("pyspark")
    sql = ModuleType("pyspark.sql")
    sql.types = FakeTypes  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    with generated_project(tmp_path, PACKAGE, files):
        for path, text in files.items():
            if "/transforms/" not in path:
                continue
            for node in ast.walk(ast.parse(text)):
                if (
                    isinstance(node, ast.ImportFrom)
                    and node.module is not None
                    and node.module.startswith(f"{PACKAGE}.pyspark.schemas.")
                ):
                    module = importlib.import_module(node.module)
                    for symbol in node.names:
                        assert hasattr(module, symbol.name), f"{path}: missing {node.module}.{symbol.name}"
