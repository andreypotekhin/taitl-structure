from dataclasses import dataclass

from structure import StructureConfig, StructureSession, StructureSources


@dataclass(frozen=True)
class FakeType:
    name: str


class FakeTypes:
    @staticmethod
    def StructType(fields):
        return FakeType("StructType")

    @staticmethod
    def StructField(name, dataType, nullable):
        return FakeType("StructField")

    @staticmethod
    def StringType():
        return FakeType("StringType")


def test_user_can_compile_memory_source_and_run_selected_transform() -> None:
    sources = StructureSources.files(
        {
            "notebook_story/schema.py": """
from structure import Schema
from structure.platform.pyspark import *

class Row(Schema):
    id = string(nullable=False)
""",
            "notebook_story/transform.py": """
from structure import Transform, input, output
from notebook_story.schema import Row

class Copy(Transform):
    rows = input(Row)
    copied = output(Row)

    def copy(self, row: Row) -> Row:
        return Row.project(row)(id=row.id)
""",
        }
    )
    session = StructureSession(
        config=StructureConfig.create(), schema_types=FakeTypes, online_executor=lambda **_: "copied"
    )

    session.compile(sources)

    assert session.run(transform="notebook_story.transform:Copy", rows="rows").copied == "copied"
