from structure.app.target.capabilities.api import Capabilities, ResolveBackendCapabilities


def test_capabilities_endpoint_returns_fresh_command_instance() -> None:
    assert isinstance(Capabilities.resolve(), ResolveBackendCapabilities)
    assert Capabilities.resolve() is not Capabilities.resolve()
