from structure.core.sources.api import DiscoverStructureSources, Sources


def test_sources_endpoint_creates_fresh_discovery_commands() -> None:
    assert isinstance(Sources().discover(), DiscoverStructureSources)
    assert Sources().discover() is not Sources().discover()
