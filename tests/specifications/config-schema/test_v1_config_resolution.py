import os
import shutil
import sys
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest

from structure import *
from structure.core.compiler.artifacts.model.CompilerOptions import CompilerOptions as CompilerArtifactOptions
from structure.core.configuration.api import ConfigError, Configuration
from structure.plugin.pyspark import *


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
        assert config.generated_docs is True
        assert config.generated_docs_dir == root / "generated" / "docs"
        assert config.generated_docs_formats == ("markdown", "json")
        assert config.generated_code_options == ()
        assert config.generated_code_hard_wrap == 120
        assert config.warn_on_udfs is True
        assert config.allow_stream_to_batch is False
        assert config.execution_mode == "online"
        assert dict(config.plugin_options["pyspark"])["profile"] == ">=3.5,<4.1"
        assert dict(config.plugin_options["pyspark"])["variant"] == "ordinary"
        assert config.hook_target_default == ("pyspark",)
        assert config.source_map["plugin"] == "default"
        assert config.source_map["generated_package"] == "default"
        assert config.source_map["generated_docs"] == "default"
        assert config.source_map["generated_docs_dir"] == "default"
        assert config.source_map["warn_on_udfs"] == "default"
        assert config.source_map["allow_stream_to_batch"] == "default"


def test_v1_config_resolves_stream_to_batch_boundary_policy() -> None:
    config = StructureConfig.create(allow_stream_to_batch=True)

    assert config.allow_stream_to_batch is True
    assert CompilerArtifactOptions.from_config(config).allow_stream_to_batch is True


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


def test_v1_config_merges_opaque_plugin_tables_and_keeps_them_immutable() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            "[tool.structure.plugin.pyspark]\nvendor_mode = \"safe\"\nretries = 2\n",
            encoding="utf-8",
        )
        (root / "pyproject.toml").write_text(
            "[tool.structure.plugin.pyspark]\nvendor_mode = \"fast\"\n"
            "[tool.structure.plugin.iterable]\nbatch_size = 100\n",
            encoding="utf-8",
        )

        config = StructureConfig.resolve(
            project_root=root,
            overrides={"plugin": {"pyspark": {"retries": 3, "feature": ["x"]}}},
        )

    assert dict(config.plugin_options["pyspark"]) == {
        "profile": ">=3.5,<4.1",
        "variant": "ordinary",
        "vendor_mode": "fast",
        "retries": 3,
        "feature": ["x"],
    }
    assert dict(config.plugin_options["iterable"]) == {"batch_size": 100}
    with pytest.raises(TypeError):
        config.plugin_options["pyspark"]["vendor_mode"] = "unsafe"  # type: ignore[index]
    with pytest.raises(TypeError):
        config.plugin_options["new"] = {}  # type: ignore[index]


def test_v5_plugin_default_and_pyspark_table_select_the_configured_target() -> None:
    config = StructureConfig.create(
        plugin={"default": "pyspark", "pyspark": {"profile": ">=4.0,<4.1", "variant": "spark-connect"}}
    )

    assert config.target == "pyspark"
    assert dict(config.plugin_options["pyspark"])["profile"] == ">=4.0,<4.1"
    assert dict(config.plugin_options["pyspark"])["variant"] == "spark-connect"


@pytest.mark.parametrize("plugin", ("wrong", {"pyspark": "wrong"}))
def test_v1_config_rejects_non_table_plugin_options(plugin) -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()

        with pytest.raises(ConfigError) as error:
            StructureConfig.resolve(project_root=root, overrides={"plugin": plugin})

    assert error.value.diagnostic.code == "CONF-E0102"
    assert error.value.diagnostic.setting == "plugin"


def test_v1_plugin_options_change_only_selected_plugin_compiler_identity() -> None:
    fast = CompilerArtifactOptions.from_config(
        StructureConfig.create(plugin={"pyspark": {"vendor_mode": "fast"}, "other": {"value": 1}})
    )
    safe = CompilerArtifactOptions.from_config(
        StructureConfig.create(plugin={"pyspark": {"vendor_mode": "safe"}, "other": {"value": 1}})
    )
    unrelated = CompilerArtifactOptions.from_config(
        StructureConfig.create(plugin={"pyspark": {"vendor_mode": "fast"}, "other": {"value": 2}})
    )

    assert fast.fingerprint() != safe.fingerprint()
    assert fast.fingerprint() == unrelated.fingerprint()


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


def test_v5_config_rejects_core_compatibility_target_matrix() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            "\n".join(
                [
                    "[tool.structure]",
                    'compat_targets = ["polars", "duckdb"]',
                    "",
                ]
            ),
            encoding="utf-8",
        )

        with pytest.raises(ConfigError) as error:
            Configuration.resolve()(project_root=root)

    assert error.value.diagnostic.setting == "compat_targets"
    assert "plugin.default" in error.value.diagnostic.use


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


def test_v1_config_accepts_generated_docs_opt_out() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            "[tool.structure]\ngenerated_docs = false\n",
            encoding="utf-8",
        )

        config = Configuration.resolve()(project_root=root)

        assert config.generated_docs is False


def test_v1_config_accepts_udf_warning_opt_out() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            "[tool.structure]\nwarn_on_udfs = false\n",
            encoding="utf-8",
        )

        config = Configuration.resolve()(project_root=root)

    assert config.warn_on_udfs is False


def test_v1_config_accepts_canonical_generated_code_options() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()

        config = StructureConfig.resolve(
            project_root=root,
            generated_code_options=["embed_exprs", "mirror_methods"],
        )

        assert config.generated_code_options == ("embed_exprs", "mirror_methods")


def test_v1_config_accepts_generated_code_hard_wrap_and_changes_compiler_fingerprint() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            "[tool.structure]\ngenerated_code_hard_wrap = 100\n",
            encoding="utf-8",
        )

        narrow = StructureConfig.resolve(project_root=root)
        default = StructureConfig.resolve(project_root=root, generated_code_hard_wrap=120)

    assert narrow.generated_code_hard_wrap == 100
    assert narrow.source_map["generated_code_hard_wrap"] == "structure.toml"
    assert (
        CompilerArtifactOptions.from_config(narrow).fingerprint()
        != CompilerArtifactOptions.from_config(default).fingerprint()
    )


@pytest.mark.parametrize("value", (79, "120", True))
def test_v1_config_rejects_invalid_generated_code_hard_wrap(value) -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()

        with pytest.raises(ConfigError) as error:
            StructureConfig.resolve(project_root=root, generated_code_hard_wrap=value)

    assert error.value.diagnostic.setting == "generated_code_hard_wrap"


def test_v1_config_resolves_embed_hooks_from_toml_and_changes_compiler_fingerprint() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\ngenerated_code_options = ["embed_hooks", "embed_exprs"]\n',
            encoding="utf-8",
        )

        embedded = StructureConfig.resolve(project_root=root)
        delegated = StructureConfig.resolve(project_root=root, generated_code_options=[])

    assert embedded.generated_code_options == ("embed_exprs", "embed_hooks")
    assert embedded.source_map["generated_code_options"] == "structure.toml"
    assert (
        CompilerArtifactOptions.from_config(embedded).fingerprint()
        != CompilerArtifactOptions.from_config(delegated).fingerprint()
    )


def test_v1_config_rejects_invalid_generated_code_options() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()

        for options, problem in ((["unknown"], "Unsupported"), (["mirror_methods", "mirror_methods"], "duplicates")):
            try:
                StructureConfig.resolve(project_root=root, generated_code_options=options)
            except ConfigError as error:
                diagnostic = error.diagnostic
            else:
                raise AssertionError("invalid generated code options should fail")

            assert diagnostic.code == "CONF-E0102"
            assert diagnostic.setting == "generated_code_options"
            assert problem in diagnostic.problem


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


def test_v5_config_rejects_legacy_target_variant() -> None:
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

        assert diagnostic.code == "CONF-E0101"
        assert diagnostic.setting == "target_variant"
        assert "plugin.pyspark" in diagnostic.use


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
        assert "plugin.pyspark" in diagnostic.use


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
