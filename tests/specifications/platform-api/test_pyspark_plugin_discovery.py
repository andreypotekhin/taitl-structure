from structure.core.platforms.api import Platform


def test_bundled_pyspark_platform_is_discoverable_from_installed_metadata() -> None:
    discovered = {platform.name for platform in Platform.registry().discover()}

    assert "pyspark" in discovered
