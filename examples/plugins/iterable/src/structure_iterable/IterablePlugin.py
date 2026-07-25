from structure.plugin.api import PluginDescriptor

from .api import PluginAPI


class IterablePlugin:
    """Entry-point object for the finite iterable starter plugin."""

    descriptor = PluginDescriptor("iterable", "Iterable", "structure-iterable-example", "0.1.0", 1, 1)

    @classmethod
    def api(cls, version: int):
        if version != 1:
            raise ValueError(f"Iterable does not support Plugin API v{version}.")
        return PluginAPI()
