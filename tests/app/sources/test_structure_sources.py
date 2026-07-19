from dataclasses import dataclass

import pytest

from structure import StructureConfig, StructureSession, StructureSources, Transform


@dataclass(frozen=True)
class FakeType:
    name: str
    args: tuple = ()


class FakeTypes:
    @staticmethod
    def StructType(fields):
        return FakeType("StructType", tuple(fields))

    @staticmethod
    def StructField(name, dataType, nullable):
        return FakeType("StructField", (name, dataType, nullable))

    @staticmethod
    def StringType():
        return FakeType("StringType")


def test_structure_sources_compile_all_transforms_and_run_registered_selection() -> None:
    sources = StructureSources.files(
        {
            "notebook_orders/schemas.py": _schemas(),
            "notebook_orders/transforms.py": _transforms("notebook_orders"),
        }
    )
    session = StructureSession(
        config=StructureConfig.create(), schema_types=FakeTypes, online_executor=lambda **_: "result"
    )

    compiled = session.compile(sources)

    assert [str(address) for address in compiled] == [
        "notebook_orders.transforms:Copy",
        "notebook_orders.transforms:CopyAgain",
    ]
    assert session.run(transform="notebook_orders.transforms:Copy", rows="rows").copied == "result"
    assert session.cache_status().entries == 2


def test_structure_sources_accumulate_and_reject_duplicate_transform_addresses() -> None:
    first = StructureSources.files(
        {"notebook_first/schemas.py": _schemas(), "notebook_first/transforms.py": _transforms("notebook_first")}
    )
    second = StructureSources.files(
        {"notebook_second/schemas.py": _schemas(), "notebook_second/transforms.py": _transforms("notebook_second")}
    )
    session = StructureSession(config=StructureConfig.create(), schema_types=FakeTypes)

    session.compile(first)
    session.compile(second)

    assert session.cache_status().entries == 4
    with pytest.raises(ValueError, match="No compiled source transform"):
        session.run(transform="notebook_missing.transforms:Copy", rows="rows")


def test_changed_source_snapshot_rebuilds_its_artifacts() -> None:
    first = StructureSources.files(
        {
            "notebook_versioned/schemas.py": _schemas(),
            "notebook_versioned/transforms.py": _transforms("notebook_versioned"),
        }
    )
    second = StructureSources.files(
        {
            "notebook_versioned/schemas.py": _schemas() + "\n# changed source snapshot\n",
            "notebook_versioned/transforms.py": _transforms("notebook_versioned"),
        }
    )
    session = StructureSession(config=StructureConfig.create(), schema_types=FakeTypes)

    session.compile(first)
    session.compile(second)

    assert session.cache_status().entries == 4


def test_base_transform_compiles_sources_and_session_load_registers_them() -> None:
    sources = StructureSources.files(
        {"notebook_loaded/schemas.py": _schemas(), "notebook_loaded/transforms.py": _transforms("notebook_loaded")}
    )
    config = StructureConfig.create()
    compiled = Transform.compile(sources, config=config, schema_types=FakeTypes)
    session = StructureSession(config=config, schema_types=FakeTypes)

    session.load(compiled)

    assert session.cache_status().entries == 2


def test_programmatic_config_has_no_source_roots() -> None:
    config = StructureConfig.create(generated_package="notebook_generated")

    assert config.source_roots == ()
    assert config.generated_package == "notebook_generated"
    assert config.source_map["generated_package"] == "programmatic"


def test_structure_sources_snapshot_a_directory(tmp_path) -> None:
    root = tmp_path / "source"
    package = root / "directory_source"
    package.mkdir(parents=True)
    (package / "schemas.py").write_text(_schemas(), encoding="utf-8")
    (package / "transforms.py").write_text(_transforms("directory_source"), encoding="utf-8")

    compiled = StructureSession(config=StructureConfig.create(), schema_types=FakeTypes).compile(
        StructureSources.from_directory(root)
    )

    assert [str(address) for address in compiled] == [
        "directory_source.transforms:Copy",
        "directory_source.transforms:CopyAgain",
    ]


def _schemas() -> str:
    return """
from structure import Schema
from structure.platform.pyspark import field

class Row(Schema):
    id = field.string(nullable=False)
"""


def _transforms(package: str) -> str:
    return f"""
from structure import Transform, input, output
from {package}.schemas import Row

class Copy(Transform):
    rows = input(Row)
    copied = output(Row)

    def copy(self, row: Row) -> Row:
        return Row.project(row)(id=row.id)

class CopyAgain(Transform):
    rows = input(Row)
    copied = output(Row)

    def copy(self, row: Row) -> Row:
        return Row.project(row)(id=row.id)
"""
