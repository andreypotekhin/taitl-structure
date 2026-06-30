from __future__ import annotations

from typing import cast

import pytest

hypothesis = pytest.importorskip("hypothesis")
strategies = pytest.importorskip("hypothesis.strategies")

from structure import String, Structure, field


@hypothesis.given(strategies.from_regex(r"[A-Za-z_][A-Za-z0-9_]{0,24}", fullmatch=True))
def test_valid_python_field_names_become_stable_structure_fields(name: str) -> None:
    schema = cast(type[Structure], type("GeneratedSchema", (Structure,), {name: field(String(), nullable=False)}))

    assert list(schema._structure_fields) == [name]
    assert schema._structure_fields[name].column == name
    assert schema._structure_fields[name].nullable is False


@hypothesis.given(strategies.text(min_size=1, max_size=24).filter(lambda text: text != "spark_column"))
def test_aliases_preserve_python_field_name_and_define_spark_column(alias: str) -> None:
    schema = cast(type[Structure], type("AliasedSchema", (Structure,), {"spark_column": field(String(), alias=alias)}))

    definition = schema._structure_fields["spark_column"]
    assert definition.name == "spark_column"
    assert definition.column == alias
