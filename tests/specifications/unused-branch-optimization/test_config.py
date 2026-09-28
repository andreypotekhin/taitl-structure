import pytest

from structure import CompilerOptions, StructureConfig, Transform, transform
from structure.core.configuration.api import ConfigError


def test_pruning_defaults_to_true_and_is_fingerprinted():
    enabled = CompilerOptions.from_config(StructureConfig.create())
    disabled = CompilerOptions.from_config(StructureConfig.create(prune_unused_steps=False))
    assert enabled.prune_unused_steps is True
    assert disabled.prune_unused_steps is False
    assert enabled.fingerprint() != disabled.fingerprint()


@pytest.mark.parametrize("value", [None, 0, 1, "true", [], {}])
def test_non_boolean_configuration_and_transform_options_fail(value):
    with pytest.raises(ConfigError):
        StructureConfig.create(prune_unused_steps=value)
    with pytest.raises(TypeError, match="prune_unused_steps must be a Boolean"):
        transform(prune_unused_steps=value)(type("Invalid", (Transform,), {}))


def test_project_value_and_programmatic_override(tmp_path):
    (tmp_path / "structure.toml").write_text('[tool.structure]\nprune_unused_steps = false\n')
    assert StructureConfig.resolve(project_root=tmp_path).prune_unused_steps is False
    assert StructureConfig.resolve(project_root=tmp_path, prune_unused_steps=True).prune_unused_steps is True
