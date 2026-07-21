from structure.core.plugins.api import Plugin


def test_bundled_pyspark_platform_is_discoverable_from_installed_metadata() -> None:
    discovered = {plugin.name for plugin in Plugin.registry().discover()}

    assert "pyspark" in discovered
