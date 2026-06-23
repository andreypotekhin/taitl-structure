import pytest

from structure import String, Structure, Transform, field, input, output, transform
from structure.app.dsl.api import compile_transform


class Raw(Structure):
    id = field(String(), nullable=False)


class Published(Structure):
    id = field(String(), nullable=False)


def test_transform_declares_named_output_contract() -> None:
    """Developers declare every transform result with an output field."""

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    plan = compile_transform(Publish)

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
        compile_transform(Publish)
