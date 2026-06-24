from structure.app.configuration.api import Configuration, ResolveStructureConfig


def test_configuration_endpoint_returns_fresh_command_instance() -> None:
    assert isinstance(Configuration.resolve(), ResolveStructureConfig)
    assert Configuration.resolve() is not Configuration.resolve()
