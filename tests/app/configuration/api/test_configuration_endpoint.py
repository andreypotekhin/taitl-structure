from structure.app.configuration.api import ResolveStructureConfig, configuration


def test_configuration_endpoint_returns_fresh_command_instance() -> None:
    assert isinstance(configuration.resolve(), ResolveStructureConfig)
    assert configuration.resolve() is not configuration.resolve()
