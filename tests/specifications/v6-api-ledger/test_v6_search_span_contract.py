"""Span-first contracts for the Search boundary relations."""

from examples.search.schemas.text import Paragraph, Section, Sentence
from examples.search.transforms.chunking.DocumentChunking import DocumentChunking
from examples.search.transforms.chunking.MaterializeText import _TextMaterializer
from examples.search.transforms.chunking.SentenceChunking import SentenceChunking
from structure import Transform


def test_text_materializer_is_a_utility_not_a_transform_base() -> None:
    assert not issubclass(_TextMaterializer, Transform)


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


def test_document_lines_use_canonical_code_point_spans() -> None:
    lines = DocumentChunking.canonical_document_lines("# 标题\r\n😀 العربية")

    assert lines[0]["line"] == "# 标题"
    assert lines[0]["span_start"] == 0
    assert lines[0]["span_end"] == len("# 标题")
    assert lines[0]["heading_span_start"] == 2
    assert lines[0]["heading_span_end"] == len("# 标题")
    assert lines[1]["span_start"] == len("# 标题\n")
    assert lines[1]["span_end"] == len("# 标题\n😀 العربية")


def test_default_sentence_spans_preserve_multilingual_code_point_ranges() -> None:
    content = "你好. 😀! العربية?"
    spans = SentenceChunking.default_sentence_spans(content)

    assert [(item["local_start"], item["local_end"], item["sentence_content"]) for item in spans] == [
        (0, len("你好."), "你好."),
        (len("你好. "), len("你好. 😀!"), "😀!"),
        (len("你好. 😀! "), len(content), "العربية?"),
    ]
