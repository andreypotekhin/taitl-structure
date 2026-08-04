import json
import os
import shutil
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from uuid import uuid4

from click.testing import CliRunner

from structure import *
from structure.core.cli.api import cli
from structure.plugin.pyspark import *


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


@dataclass(frozen=True)
class FakeType:
    name: str
    args: tuple = ()
    options: tuple = ()


class FakeTypes:

    @staticmethod
    def StructType(fields):
        return FakeType("StructType", tuple(fields))

    @staticmethod
    def StructField(name, dataType, nullable):
        return FakeType("StructField", (name, dataType, nullable))

    @staticmethod
    def StringType():
        return FakeType("StringType")

    @staticmethod
    def DecimalType(precision, scale):
        return FakeType("DecimalType", (precision, scale))


class CaptureStorage:

    def __init__(self) -> None:
        self.files: dict[str, str] = {}

    def write(self, files: dict[str, str]) -> str:
        self.files = dict(files)
        return "captured"


def drop_orders_modules() -> None:
    for name in list(sys.modules):
        if name == "orders" or name.startswith("orders."):
            sys.modules.pop(name, None)


def write_project(root: Path) -> None:
    drop_orders_modules()
    package = root / "src" / "orders"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "schemas.py").write_text(
        "\n".join(
            [
                "from structure import *",
                "from structure.plugin.pyspark import *",
                "",
                "class OrderRaw(Schema):",
                "    id = string(nullable=False)",
                "    total = string(nullable=True)",
                "",
                "class OrderNormalized(Schema):",
                "    id = string(nullable=False)",
                "    total = decimal(12, 2, nullable=False)",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (package / "transforms.py").write_text(
        "\n".join(
            [
                "from structure import *",
                "from structure.plugin.pyspark import *",
                "from orders.schemas import OrderNormalized, OrderRaw",
                "",
                "@transform",
                "class NormalizeOrders(Transform):",
                "    orders = input(OrderRaw)",
                "    normalized = output(OrderNormalized)",
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


def append_second_transform(root: Path) -> None:
    (root / "src" / "orders" / "transforms.py").write_text(
        (root / "src" / "orders" / "transforms.py").read_text(encoding="utf-8")
        + "\n".join(
            [
                "",
                "@transform",
                "class PublishOrders(Transform):",
                "    orders = input(OrderRaw)",
                "    published = output(OrderNormalized)",
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


def write_optional_transform_project(root: Path) -> None:
    drop_orders_modules()
    package = root / "src" / "orders"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "schemas.py").write_text(
        "\n".join(
            [
                "from structure import *",
                "from structure.plugin.pyspark import *",
                "",
                "class OrderRaw(Schema):",
                "    id = string(nullable=False)",
                "    total = string(nullable=True)",
                "",
                "class OrderNormalized(Schema):",
                "    id = string(nullable=False)",
                "    total = decimal(12, 2, nullable=False)",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (package / "transforms.py").write_text(
        "\n".join(
            [
                "from structure import *",
                "from structure.plugin.pyspark import *",
                "from orders.schemas import OrderNormalized, OrderRaw",
                "",
                "class NormalizeBase(Transform):",
                "    orders = input(OrderRaw)",
                "    prepared = lane(OrderNormalized)",
                "",
                "    @step(output=prepared)",
                "    def prepare(self, order: OrderRaw) -> OrderNormalized:",
                "        where(order.id.is_not_null())",
                "        return OrderNormalized(",
                "            id=order.id,",
                "            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),",
                "        )",
                "",
                "class NormalizeOrders(NormalizeBase):",
                "    normalized = output(OrderNormalized)",
                "",
                "    def normalize(self, order: OrderNormalized) -> OrderNormalized:",
                "        return OrderNormalized(id=order.id, total=order.total)",
                "",
                "class NormalizePipeline(Transform):",
                "    orders = input(OrderRaw)",
                "",
                "    pipeline = Transform.to(NormalizeOrders(orders=orders))",
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
    assert "--target TEXT" in CliRunner().invoke(cli, ["check", "--help"]).output


def test_v1_cli_init_writes_seed_config() -> None:
    with workspace_tmp():
        result = CliRunner().invoke(cli, ["init", "--seed-config"])

        assert result.exit_code == 0
        assert Path("structure.toml").exists()
        text = Path("structure.toml").read_text(encoding="utf-8")
        assert 'generated_package = "structure_generated"' in text
        assert "generated_docs = true" in text
        assert 'generated_docs_dir = "docs"' in text
        assert 'generated_docs_formats = ["markdown", "json"]' in text
        assert '[tool.structure.plugin]' in text
        assert 'default = "pyspark"' in text
        assert '[tool.structure.plugin.pyspark]' in text
        assert 'profile = ">=3.5,<4.1"' in text
        assert 'variant = "ordinary"' in text


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


def test_v1_cli_check_discovers_plain_concrete_transforms_only() -> None:
    with workspace_tmp() as root:
        write_optional_transform_project(root)

        result = CliRunner().invoke(cli, ["check"])

        assert result.exit_code == 0, result.output
        assert "Structure check passed" in result.output
        assert "transforms: 2" in result.output
        assert not Path("generated").exists()


def test_v5_cli_rejects_removed_backend_compatibility_options() -> None:
    result = CliRunner().invoke(cli, ["check", "--target-profile", ">=3.5,<4.1"])

    assert result.exit_code == 2
    assert "No such option '--target-profile'" in result.output


def test_v1_cli_compile_writes_generated_files_and_fail_on_diff_passes() -> None:
    with workspace_tmp() as root:
        write_project(root)

        compiled = CliRunner().invoke(cli, ["compile"])
        checked = CliRunner().invoke(cli, ["compile", "--fail-on-diff"])

        assert compiled.exit_code == 0, compiled.output
        assert checked.exit_code == 0, checked.output
        assert Path("generated/structure_generated/pyspark/transforms/orders/transforms.py").exists()
        assert Path("generated/docs/index.md").exists()
        assert Path("generated/docs/schemas/OrderRaw.md").exists()
        assert Path("generated/docs/transforms/orders.transforms.NormalizeOrders.json").exists()
        assert "files written:" in compiled.output
        assert "generated docs dir: generated/docs" in compiled.output


def test_v1_cli_compile_writes_one_transform_module_per_source_unit() -> None:
    with workspace_tmp() as root:
        write_project(root)
        append_second_transform(root)

        result = CliRunner().invoke(cli, ["compile"])

        text = Path("generated/structure_generated/pyspark/transforms/orders/transforms.py").read_text(encoding="utf-8")
        assert result.exit_code == 0, result.output
        assert "class NormalizeOrdersGenerated" in text
        assert "class PublishOrdersGenerated" in text


def test_v1_transform_generate_writes_one_transform_module_per_source_unit() -> None:
    with workspace_tmp() as root:
        write_project(root)
        append_second_transform(root)
        sys.path.insert(0, str(root / "src"))
        try:
            module = import_module("orders.transforms")
            storage = CaptureStorage()

            generated = module.NormalizeOrders.generate(
                project_root=root,
                storage=storage,
                schema_types=FakeTypes,
            )

            text = storage.files["structure_generated/pyspark/transforms/orders/transforms.py"]
            assert generated.source_unit == "orders.transforms"
            assert generated.module_name == "structure_generated.pyspark.transforms.orders.transforms"
            assert generated.classes == ("NormalizeOrdersGenerated", "PublishOrdersGenerated")
            assert generated.result == "captured"
            assert "class NormalizeOrdersGenerated" in text
            assert "class PublishOrdersGenerated" in text
        finally:
            if str(root / "src") in sys.path:
                sys.path.remove(str(root / "src"))
            drop_orders_modules()


def test_v1_disk_storage_imports_from_generated_root() -> None:
    with workspace_tmp() as root:
        storage = DiskStorage(root / "generated")
        storage.write(
            {
                "pkg/__init__.py": "",
                "pkg/mod.py": "VALUE = 42\n",
            }
        )

        module = storage.import_module("pkg.mod")

        assert module.VALUE == 42


def test_v1_cli_compile_writes_generated_docs_contract() -> None:
    with workspace_tmp() as root:
        write_project(root)

        result = CliRunner().invoke(cli, ["compile"])

        schema = Path("generated/docs/schemas/OrderRaw.md").read_text(encoding="utf-8")
        transform = json.loads(Path("generated/docs/transforms/orders.transforms.NormalizeOrders.json").read_text())
        assert result.exit_code == 0, result.output
        assert "# OrderRaw" in schema
        assert "| `id` | `id` | `string` | no |" in schema
        assert transform["generated_by"] == "Structure"
        assert transform["name"] == "NormalizeOrders"
        assert transform["inputs"] == [{"name": "orders", "ordinal": 0, "schema": "OrderRaw"}]
        assert transform["outputs"] == [{"name": "normalized", "ordinal": 0, "schema": "OrderNormalized"}]
        assert transform["step_methods"][0]["name"] == "normalize"
        assert transform["target_artifacts"]["pyspark_transform"] == "pyspark/transforms/orders/transforms.py"


def test_v1_cli_compile_respects_generated_docs_format_override() -> None:
    with workspace_tmp() as root:
        write_project(root)

        result = CliRunner().invoke(cli, ["compile", "--generated-docs-formats", "json"])

        assert result.exit_code == 0, result.output
        assert Path("generated/docs/index.json").exists()
        assert not Path("generated/docs/index.md").exists()


def test_v1_cli_compile_allows_generated_docs_opt_out() -> None:
    with workspace_tmp() as root:
        write_project(root)

        result = CliRunner().invoke(cli, ["compile", "--no-generated-docs"])

        assert result.exit_code == 0, result.output
        assert Path("generated/structure_generated/pyspark/transforms/orders/transforms.py").exists()
        assert not Path("generated/docs").exists()
        assert "generated docs: disabled" in result.output


def test_v1_cli_fail_on_diff_ignores_existing_docs_when_docs_are_disabled() -> None:
    with workspace_tmp() as root:
        write_project(root)
        CliRunner().invoke(cli, ["compile"])

        result = CliRunner().invoke(cli, ["compile", "--fail-on-diff", "--no-generated-docs"])

        assert result.exit_code == 0, result.output
        assert Path("generated/docs/index.md").exists()


def test_v1_cli_fail_on_diff_reports_stale_generated_output_without_writing() -> None:
    with workspace_tmp() as root:
        write_project(root)
        CliRunner().invoke(cli, ["compile"])
        target = Path("generated/structure_generated/pyspark/transforms/orders/transforms.py")
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
        assert "operations: filter(row_filtering)" in result.output
        assert "traceability:" in result.output
        assert "static dataflow:" in result.output
        assert "NormalizeOrders <- orders" in result.output


def test_v5_cli_rejects_removed_compatibility_target_option() -> None:
    result = CliRunner().invoke(cli, ["explain", "--compat-targets", "polars,duckdb", "orders.transforms.NormalizeOrders"])

    assert result.exit_code == 2
    assert "No such option '--compat-targets'" in result.output


def test_v1_cli_clean_removes_owned_generated_files_only() -> None:
    with workspace_tmp() as root:
        write_project(root)
        CliRunner().invoke(cli, ["compile"])
        manual = Path("generated/manual.txt")
        manual.write_text("do not remove\n", encoding="utf-8")

        result = CliRunner().invoke(cli, ["clean"])

        assert result.exit_code == 0
        assert manual.exists()
        assert not Path("generated/structure_generated/pyspark/transforms/orders/transforms.py").exists()


def test_v1_cli_unexpected_failure_renders_internal_diagnostic(mocker) -> None:
    module = import_module("structure.core.cli.api.cli")
    mocker.patch.object(module.CliApp, "resolve_config", side_effect=RuntimeError("boom"))

    result = CliRunner().invoke(cli, ["check"])

    assert result.exit_code == 1
    assert "CLI-X1101" in result.output
    assert "Unexpected internal failure" in result.output
    assert "docs/Diagnostics.md#cli-x1101" in result.output
