import pytest

from examples.search.algorithms.field_search import parse_field_search_query
from examples.search.transforms.fields import ExtractDocumentFields
from examples.search.transforms.indexing import FieldIndex
from examples.search.transforms.searching.search_fields import SearchFields
from structure.core.compiler.api import Compiler


def test_field_query_parser_forwards_body_and_normalizes_metadata() -> None:
    parsed = parse_field_search_query("q", 'title:"Release the Notes" and content:upgrade')

    assert parsed.query["content"] == "upgrade"
    assert parsed.query["clause_count"] == 1
    assert parsed.query["requires_content"] is True
    assert [term["term"] for term in parsed.terms] == ["release", "notes"]
    assert [term["term_ordinal"] for term in parsed.terms] == [0, 2]
    assert all(term["is_phrase"] for term in parsed.terms)
    implicit = parse_field_search_query("q2", "release notes")
    assert implicit.query["operator"] == "and"
    assert implicit.query["content"] == "release notes"
    assert implicit.query["clause_count"] == 0
    assert implicit.query["requires_content"] is True
    assert implicit.terms == ()


def test_field_query_parser_preserves_body_source_order_and_requires_prefixes() -> None:
    parsed = parse_field_search_query("q", "aurora title:guide beacon content:upgrade")

    assert parsed.query["content"] == "aurora beacon upgrade"
    assert [(term["field_name"], term["term"]) for term in parsed.terms] == [("title", "guide")]


def test_field_query_parser_rejects_mixed_or_content_and_uppercase_operators() -> None:
    with pytest.raises(ValueError, match="mixed boolean operators"):
        parse_field_search_query("q", "title:release or category_id:docs and source:guide")
    with pytest.raises(ValueError, match="lowercase"):
        parse_field_search_query("q", "title:release AND source:guide")
    with pytest.raises(ValueError, match="content:"):
        parse_field_search_query("q", "title:release or content:upgrade")


@pytest.mark.parametrize("transform", [ExtractDocumentFields, FieldIndex, SearchFields])
def test_field_search_transforms_compile(transform) -> None:
    Compiler.frontend.compile()(transform, materialize_schemas=False)
