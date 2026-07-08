import os
import shutil
import sys
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from structure import StructureConfig
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
        assert config.generated_docs_dir == root / "generated" / "docs"
        assert config.generated_docs_formats == ("markdown", "json")
        assert config.execution_mode == "online"
        assert config.target_profile == ">=3.5,<4.1"
        assert config.target_variant == "ordinary"
        assert config.compat_targets == ()
        assert config.hook_target_default == ("pyspark",)
        assert config.source_map["target_profile"] == "default"
        assert config.source_map["target_variant"] == "default"
        assert config.source_map["generated_package"] == "default"
        assert config.source_map["generated_docs_dir"] == "default"


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


def test_v1_programmatic_config_precedence_is_api_pyproject_structure_defaults() -> None:
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

        config = StructureConfig.resolve(project_root=root, generated_package="from_programmatic")

        assert config.generated_package == "from_programmatic"
        assert config.traceability == "none"
        assert config.source_map["generated_package"] == "programmatic"
        assert config.source_map["traceability"] == "structure.toml"


def test_v1_programmatic_config_accepts_mapping_overrides_for_dotted_keys() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()

        config = StructureConfig.resolve(
            project_root=root,
            overrides={"spark.sql.ansi.enabled": False},
        )

        assert config.spark_sql["spark.sql.ansi.enabled"] is False
        assert config.source_map["spark.sql.ansi.enabled"] == "programmatic"


def test_v1_programmatic_config_rejects_duplicate_override_sources() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()

        try:
            StructureConfig.resolve(
                project_root=root, overrides={"generated_package": "first"}, generated_package="next"
            )
        except ValueError as error:
            message = str(error)
        else:
            raise AssertionError("duplicate programmatic overrides should fail")

        assert "generated_package" in message


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


def test_v1_config_accepts_target_profile_and_future_backend_fields() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            "\n".join(
                [
                    "[tool.structure]",
                    'target_profile = ">=3.5,<4.1"',
                    'target_variant = "spark-connect"',
                    'compat_targets = ["polars", "duckdb"]',
                    'hook_target_default = ["pyspark"]',
                    "",
                ]
            ),
            encoding="utf-8",
        )

        config = Configuration.resolve()(project_root=root)

        assert config.target_backend == "pyspark"
        assert config.target_profile == ">=3.5,<4.1"
        assert config.target_variant == "spark-connect"
        assert config.compat_targets == ("polars", "duckdb")
        assert config.hook_target_default == ("pyspark",)


def test_v1_config_accepts_generated_docs_settings() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            "\n".join(
                [
                    "[tool.structure]",
                    'generated_docs_dir = "reference"',
                    'generated_docs_formats = ["json"]',
                    "",
                ]
            ),
            encoding="utf-8",
        )

        config = Configuration.resolve()(project_root=root)

        assert config.generated_docs_dir == root / "generated" / "reference"
        assert config.generated_docs_formats == ("json",)


def test_v1_config_rejects_generated_docs_dir_escape() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\ngenerated_docs_dir = "../docs"\n',
            encoding="utf-8",
        )

        try:
            Configuration.resolve()(project_root=root)
        except ConfigError as error:
            diagnostic = error.diagnostic
        else:
            raise AssertionError("generated docs dir should stay inside generated_dir")

        assert diagnostic.code == "CONF-E0102"
        assert diagnostic.setting == "generated_docs_dir"


def test_v1_config_rejects_unknown_generated_docs_format() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\ngenerated_docs_formats = ["html"]\n',
            encoding="utf-8",
        )

        try:
            Configuration.resolve()(project_root=root)
        except ConfigError as error:
            diagnostic = error.diagnostic
        else:
            raise AssertionError("unknown generated docs format should fail")

        assert diagnostic.code == "CONF-E0102"
        assert diagnostic.setting == "generated_docs_formats"
        assert "markdown, json" in diagnostic.use


def test_v1_config_rejects_unknown_target_variant() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\ntarget_variant = "classic"\n',
            encoding="utf-8",
        )

        try:
            Configuration.resolve()(project_root=root)
        except ConfigError as error:
            diagnostic = error.diagnostic
        else:
            raise AssertionError("unknown target_variant should fail")

        assert diagnostic.code == "CONF-E0102"
        assert diagnostic.setting == "target_variant"
        assert "ordinary, spark-connect" in diagnostic.use


def test_v1_config_rejects_legacy_target_pyspark_key() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\ntarget_pyspark = ">=3.5,<4.1"\n',
            encoding="utf-8",
        )

        try:
            Configuration.resolve()(project_root=root)
        except ConfigError as error:
            diagnostic = error.diagnostic
        else:
            raise AssertionError("target_pyspark should be rejected after the target_profile rename")

        assert diagnostic.code == "CONF-E0101"
        assert diagnostic.setting == "target_pyspark"
        assert "target_profile" in diagnostic.use


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
