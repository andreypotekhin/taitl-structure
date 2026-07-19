import json
import os
import shutil
import sys
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from click.testing import CliRunner

from structure.core.cli.api import cli


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


def test_generated_docs_make_transform_contract_readable() -> None:
    """A developer can generate schema and transform docs without inspecting generated PySpark."""
    with workspace_tmp() as root:
        _write_project(root)

        result = CliRunner().invoke(cli, ["compile"])

        index = Path("generated/docs/index.md").read_text(encoding="utf-8")
        schema = Path("generated/docs/schemas/OrderRaw.md").read_text(encoding="utf-8")
        transform = json.loads(Path("generated/docs/transforms/orders.transforms.NormalizeOrders.json").read_text())
        assert result.exit_code == 0, result.output
        assert "[OrderRaw](schemas/OrderRaw.md)" in index
        assert "[NormalizeOrders](transforms/orders.transforms.NormalizeOrders.md)" in index
        assert "| `total` | `total` | `string` | yes |" in schema
        assert transform["inputs"] == [{"name": "orders", "ordinal": 0, "schema": "OrderRaw"}]
        assert transform["outputs"] == [{"name": "normalized", "ordinal": 0, "schema": "OrderNormalized"}]
        assert transform["step_methods"][0]["input_schema"] == "OrderRaw"
        assert transform["step_methods"][0]["output_schema"] == "OrderNormalized"


def _write_project(root: Path) -> None:
    _drop_orders_modules()
    package = root / "src" / "orders"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "schemas.py").write_text(
        "\n".join(
            [
                "from structure import Schema",
                "from structure.platform.pyspark.dsl.field import *",
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
                "from structure import Transform, coalesce, input, output, to_decimal, transform, where",
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


def _drop_orders_modules() -> None:
    for name in list(sys.modules):
        if name == "orders" or name.startswith("orders."):
            sys.modules.pop(name, None)
