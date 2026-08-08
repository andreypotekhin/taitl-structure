"""Span-first contracts for the Search boundary relations."""

from examples.search.schemas.text import Paragraph, Section, Sentence
from examples.search.transforms.chunking.DocumentChunking import DocumentChunking
from examples.search.transforms.chunking.SentenceChunking import SentenceChunking
from examples.search.transforms.lib.Text import Text
from structure import Transform


def test_text_is_a_utility_not_a_transform_base() -> None:
    assert not issubclass(Text, Transform)


def test_search_boundaries_are_flat_and_text_free() -> None:
    assert tuple(Section._structure_fields) == (
        "id",
        "document_id",
        "ordinal",
        "span_start",
        "span_end",
        "heading_span_start",
        "heading_span_end",
    )
    assert tuple(Paragraph._structure_fields) == (
        "id",
        "document_id",
        "section_id",
        "ordinal",
        "span_start",
        "span_end",
    )
    assert tuple(Sentence._structure_fields) == (
        "id",
        "document_id",
        "section_id",
        "paragraph_id",
        "paragraph_ordinal",
        "ordinal",
        "span_start",
        "span_end",
    )


def test_document_chunking_uses_compiler_visible_line_expansion() -> None:
    assert not hasattr(DocumentChunking, "canonical_document_lines")


def test_default_sentence_spans_preserve_multilingual_code_point_ranges() -> None:
    content = "你好. 😀! العربية?"
    spans = SentenceChunking.default_sentence_spans(content)

    assert [(item["local_start"], item["local_end"], item["sentence_content"]) for item in spans] == [
        (0, len("你好."), "你好."),
        (len("你好. "), len("你好. 😀!"), "😀!"),
        (len("你好. 😀! "), len(content), "العربية?"),
    ]
