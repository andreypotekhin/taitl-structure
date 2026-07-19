from types import SimpleNamespace

from structure.platform.api import PLATFORM_ENTRY_POINT_GROUP


class BundledPySparkEntry:
    group = PLATFORM_ENTRY_POINT_GROUP
    name = "pyspark"
    dist = SimpleNamespace(name="structure")

    def load(self):
        from structure.platform.pyspark import PySparkPlatform

        return PySparkPlatform
