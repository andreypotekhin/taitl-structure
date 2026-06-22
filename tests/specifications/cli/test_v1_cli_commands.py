import os
import shutil
import sys
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from uuid import uuid4

from click.testing import CliRunner

from structure.app.cli.api import cli


@contextmanager
def workspace_tmp():
    root = (Path(".pytest-workspace-tmp") / uuid4().hex).resolve()
    root.mkdir(parents=True)
    old = Path.cwd()
    try:
        os.chdir(root)
        yield root
    finally:
        os.chdir(old)
        shutil.rmtree(root, ignore_errors=True)


def write_project(root: Path) -> None:
    package = root / "src" / "orders"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "schemas.py").write_text(
        "\n".join(
            [
                "from structure import Decimal, String, Structure, field",
                "",
                "class OrderRaw(Structure):",
                "    id = field(String(), nullable=False)",
                "    total = field(String(), nullable=True)",
                "",
                "class OrderNormalized(Structure):",
                "    id = field(String(), nullable=False)",
                "    total = field(Decimal(12, 2), nullable=False)",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (package / "transforms.py").write_text(
        "\n".join(
            [
                "from structure import Transform, coalesce, input, to_decimal, transform, where",
                "from orders.schemas import OrderNormalized, OrderRaw",
                "",
                "@transform",
                "class NormalizeOrders(Transform):",
                "    orders = input(OrderRaw)",
                "",
                "    def normalize(self, order: OrderRaw) -> OrderNormalized:",
                "        where(order.id.is_not_null())",
                "        return OrderNormalized(",
                "            id=order.id,",
                "            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),",
                "        )",
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_v1_cli_help_lists_commands() -> None:
    result = CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "check" in result.output
    assert "compile" in result.output
    assert "explain" in result.output


def test_v1_cli_init_writes_seed_config() -> None:
    with workspace_tmp():
        result = CliRunner().invoke(cli, ["init", "--seed-config"])

        assert result.exit_code == 0
        assert Path("structure.toml").exists()
        assert 'generated_package = "structure_generated"' in Path("structure.toml").read_text(encoding="utf-8")


def test_v1_cli_check_is_spark_free_and_does_not_write_generated_files() -> None:
    with workspace_tmp() as root:
        write_project(root)
        before = {name for name in sys.modules if name.startswith("pyspark")}

        result = CliRunner().invoke(cli, ["check"])

        after = {name for name in sys.modules if name.startswith("pyspark")}
        assert result.exit_code == 0, result.output
        assert "Structure check passed" in result.output
        assert "transforms: 1" in result.output
        assert after == before
        assert not Path("generated").exists()


def test_v1_cli_compile_writes_generated_files_and_fail_on_diff_passes() -> None:
    with workspace_tmp() as root:
        write_project(root)

        compiled = CliRunner().invoke(cli, ["compile"])
        checked = CliRunner().invoke(cli, ["compile", "--fail-on-diff"])

        assert compiled.exit_code == 0, compiled.output
        assert checked.exit_code == 0, checked.output
        assert Path("generated/structure_generated/pyspark/transforms/transforms.py").exists()
        assert "files written:" in compiled.output


def test_v1_cli_fail_on_diff_reports_stale_generated_output_without_writing() -> None:
    with workspace_tmp() as root:
        write_project(root)
        CliRunner().invoke(cli, ["compile"])
        target = Path("generated/structure_generated/pyspark/transforms/transforms.py")
        original = target.read_text(encoding="utf-8")
        target.write_text(original + "\n# stale edit\n", encoding="utf-8")

        result = CliRunner().invoke(cli, ["compile", "--fail-on-diff"])

        assert result.exit_code == 1
        assert "GEN-E0901" in result.output
        assert "Generated output is stale" in result.output
        assert "docs/Diagnostics.md#gen-e0901" in result.output
        assert target.read_text(encoding="utf-8").endswith("# stale edit\n")


def test_v1_cli_explain_renders_transform_plan() -> None:
    with workspace_tmp() as root:
        write_project(root)

        result = CliRunner().invoke(cli, ["explain", "orders.transforms.NormalizeOrders"])

        assert result.exit_code == 0, result.output
        assert "NormalizeOrders" in result.output
        assert "streaming:" in result.output
        assert "status: compatible" in result.output
        assert "orders: OrderRaw" in result.output
        assert "normalize: OrderRaw -> OrderNormalized" in result.output
        assert "traceability:" in result.output
        assert "static dataflow:" in result.output
        assert "NormalizeOrders <- orders" in result.output


def test_v1_cli_clean_removes_owned_generated_files_only() -> None:
    with workspace_tmp() as root:
        write_project(root)
        CliRunner().invoke(cli, ["compile"])
        manual = Path("generated/manual.txt")
        manual.write_text("do not remove\n", encoding="utf-8")

        result = CliRunner().invoke(cli, ["clean"])

        assert result.exit_code == 0
        assert manual.exists()
        assert not Path("generated/structure_generated/pyspark/transforms/transforms.py").exists()


def test_v1_cli_unexpected_failure_renders_internal_diagnostic(mocker) -> None:
    module = import_module("structure.app.cli.api.cli")
    mocker.patch.object(module, "resolve_structure_config", side_effect=RuntimeError("boom"))

    result = CliRunner().invoke(cli, ["check"])

    assert result.exit_code == 1
    assert "CLI-X1101" in result.output
    assert "Unexpected internal failure" in result.output
    assert "docs/Diagnostics.md#cli-x1101" in result.output
