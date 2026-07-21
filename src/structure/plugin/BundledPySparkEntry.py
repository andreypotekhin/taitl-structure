from types import SimpleNamespace

from structure.plugin.api import PLUGIN_ENTRY_POINT_GROUP


class BundledPySparkEntry:
    group = PLUGIN_ENTRY_POINT_GROUP
    name = "pyspark"
    dist = SimpleNamespace(name="structure")

    def load(self):
        from structure.plugin.pyspark import PySparkPlugin

        return PySparkPlugin
