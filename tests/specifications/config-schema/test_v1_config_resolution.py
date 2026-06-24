import os
import shutil
import sys
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from structure.app.configuration.api import ConfigError, Configuration


@contextmanager
def workspace_tmp():
    root = Path(".pytest-workspace-tmp") / uuid4().hex
    root.mkdir(parents=True)
    try:
        yield root.resolve()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_v1_config_uses_defaults_and_tracks_sources() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()

        config = Configuration.resolve()(project_root=root)

        assert [path.name for path in config.source_roots] == ["src"]
        assert config.generated_package == "structure_generated"
        assert config.execution_mode == "online"
        assert config.source_map["generated_package"] == "default"


def test_v1_config_precedence_is_cli_pyproject_structure_defaults() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\ngenerated_package = "from_structure"\ntraceability = "none"\n',
            encoding="utf-8",
        )
        (root / "pyproject.toml").write_text(
            '[tool.structure]\ngenerated_package = "from_pyproject"\n',
            encoding="utf-8",
        )

        config = Configuration.resolve()(
            project_root=root,
            overrides={"generated_package": "from_cli"},
        )

        assert config.generated_package == "from_cli"
        assert config.traceability == "none"
        assert config.source_map["generated_package"] == "CLI"
        assert config.source_map["traceability"] == "structure.toml"


def test_v1_config_unknown_key_suggests_known_key() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\ngeneratedDirectory = "generated"\n',
            encoding="utf-8",
        )

        try:
            Configuration.resolve()(project_root=root)
        except ConfigError as error:
            diagnostic = error.diagnostic
        else:
            raise AssertionError("unknown config key should fail")

        assert diagnostic.code == "CONF-E0101"
        assert diagnostic.setting == "generatedDirectory"
        assert "generated_dir" in diagnostic.use


def test_v1_config_invalid_values_fail_before_discovery() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\ntraceability = "fieldz"\n',
            encoding="utf-8",
        )

        try:
            Configuration.resolve()(project_root=root)
        except ConfigError as error:
            diagnostic = error.diagnostic
        else:
            raise AssertionError("invalid traceability should fail")

        assert diagnostic.code == "CONF-E0102"
        assert diagnostic.setting == "traceability"
        assert diagnostic.docs == "docs/Diagnostics.md#conf-e0102"
        assert "none, compiler, columns, debug" in diagnostic.use


def test_v1_config_rejects_generated_package_structure() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()

        try:
            Configuration.resolve()(project_root=root, overrides={"generated_package": "structure"})
        except ConfigError as error:
            diagnostic = error.diagnostic
        else:
            raise AssertionError("generated package should not collide with structure")

        assert diagnostic.code == "CONF-E0102"
        assert diagnostic.setting == "generated_package"
        assert "structure_generated" in diagnostic.use


def test_v1_config_does_not_import_pyspark() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        before = {name for name in sys.modules if name.startswith("pyspark")}

        Configuration.resolve()(project_root=root)

        after = {name for name in sys.modules if name.startswith("pyspark")}
        assert after == before
