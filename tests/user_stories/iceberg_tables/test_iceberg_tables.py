"""Acceptance coverage for the Iceberg table-transform stories."""

from typing import Any, cast

from structure import Schema, Transform, transform
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import iceberg_delete, iceberg_table, integer


class Account(Schema):
    id = integer(nullable=False)


@transform
class DeleteAccount(Transform):
    accounts = iceberg_table(Account)

    def delete(self, account: Account) -> None:
        iceberg_delete(account, where=account.id == 0)


def test_iceberg_story_uses_a_caller_owned_catalog_table_binding() -> None:
    table_name = "lakehouse.finance.accounts"
    invocation = DeleteAccount(accounts=table_name)
    plan = cast(
        Any,
        Compiler.frontend.compile()(DeleteAccount, materialize_schemas=False, plugin={"pyspark": {}}).lowered,
    )

    assert invocation._structure_bound_inputs["accounts"] == table_name
    assert plan.inputs[0].binding == "iceberg_table"
    assert plan.steps[0].delta_mutations[0].kind == "iceberg_delete"
