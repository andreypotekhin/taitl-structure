from typing import cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark import *


class Raw(Schema):
    id = string(nullable=False)


class Published(Schema):
    id = string(nullable=False)


def test_transform_declares_named_output_contract() -> None:
    """Developers declare every transform result with an output field."""

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    plan = cast(TransformPlan, Compiler.frontend.compile()(Publish, materialize_schemas=False).analysis)

    assert [item.name for item in plan.outputs] == ["published"]
    assert plan.output_schema is Published


def test_transform_without_output_contract_fails_early() -> None:
    """Transforms without an output field fail before symbolic execution."""

    @transform
    class Publish(Transform):
        rows = input(Raw)

        def publish(self, row: Raw) -> Published:
            raise AssertionError("symbolic execution must not start")

    with pytest.raises(Exception, match="Publish declares no outputs"):
        Compiler.frontend.compile()(Publish, materialize_schemas=False)
