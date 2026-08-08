"""Default, replaceable sentence chunking for Search paragraph spans."""

from typing import Any

from examples.search.schemas.chunking.intermediate import ExpandedSentenceText, SentenceText
from examples.search.schemas.text import Document, Paragraph, Sentence
from examples.search.transforms.chunking.MaterializeText import _TextMaterializer
from structure import Transform, input, output, special, step, transform
from structure.plugin.pyspark import concat_ws, inner_join, posexplode_struct, regexp_replace, types, where


@transform(warn_on_udfs=False)
class SentenceChunking(Transform):
    """Supply default sentence chunks; callers may replace this transform for exact segmentation."""

    documents = input(Document)
    paragraphs = input(Paragraph)
    sentences = output(Sentence)

    @special(
        type="udf",
        return_type=types.array(types.struct(SentenceText), contains_null=False),
        nullable=False,
    )
    def default_sentence_spans(content: Any) -> list[dict[str, object]]:
        """Split on terminal punctuation while retaining paragraph-local source spans."""
        import re

        spans: list[dict[str, object]] = []
        cursor = 0
        for separator in re.finditer(r"(?<=[.!?])\s+", content):
            start, end = cursor, separator.start()
            while start < end and content[start].isspace():
                start += 1
            while end > start and content[end - 1].isspace():
                end -= 1
            if start < end:
                spans.append({"local_start": start, "local_end": end, "sentence_content": content[start:end]})
            cursor = separator.end()
        start, end = cursor, len(content)
        while start < end and content[start].isspace():
            start += 1
        while end > start and content[end - 1].isspace():
            end -= 1
        if start < end:
            spans.append({"local_start": start, "local_end": end, "sentence_content": content[start:end]})
        return spans

    @step(input=[documents, paragraphs], output=sentences)
    def chunk(self, document: Document, paragraph: Paragraph) -> Sentence:
        inner_join(on=document.id == paragraph.document_id)
        content = regexp_replace(
            _TextMaterializer.canonical_span(document.content, paragraph.span_start, paragraph.span_end),
            pattern="\n",
            replacement=" ",
        )
        sentence = posexplode_struct(
            self.default_sentence_spans(content),
            as_=ExpandedSentenceText,
            ordinal="position",
            scope="sentence_text",
        )
        where(sentence.sentence_content != "")
        return Sentence(
            id=concat_ws("#s", paragraph.id, sentence.position.cast(types.string())),
            document_id=paragraph.document_id,
            section_id=paragraph.section_id,
            paragraph_id=paragraph.id,
            paragraph_ordinal=paragraph.ordinal,
            ordinal=(sentence.position + 1).cast(types.integer()),
            span_start=paragraph.span_start + sentence.local_start,
            span_end=paragraph.span_start + sentence.local_end,
        )
