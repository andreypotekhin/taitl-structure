import sys

from structure import StructureTools
from structure.app.tools.api import StructureTools as AppStructureTools


def test_structure_tools_is_public_and_import_safe() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    assert StructureTools is AppStructureTools

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
