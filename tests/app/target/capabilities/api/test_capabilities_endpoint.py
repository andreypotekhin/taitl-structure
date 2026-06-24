from structure.app.target.capabilities.api import ResolveBackendCapabilities, capabilities


def test_capabilities_endpoint_returns_fresh_command_instance() -> None:
    assert isinstance(capabilities.resolve(), ResolveBackendCapabilities)
    assert capabilities.resolve() is not capabilities.resolve()
