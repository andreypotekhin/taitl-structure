"""Default, replaceable sentence chunking for Search paragraphs."""

from typing import Any

from examples.search.schemas.chunking.chunk import ExpandedSentenceText, SentenceText
from examples.search.schemas.text import Paragraph, Sentence
from structure import Transform, input, output, special, step, transform
from structure.plugin.pyspark import arr_transform, concat_ws, posexplode_struct, trim, types, where


@transform(warn_on_udfs=False)
class SentenceChunking(Transform):
    """Supply default sentence chunks; callers may replace this transform for exact segmentation."""

    paragraphs = input(Paragraph)
    sentences = output(Sentence)

    @special(type="udf", return_type=types.array(types.string(), contains_null=False), nullable=False)
    def default_sentence_texts(content: Any) -> list[str]:
        """Split on terminal punctuation; inaccurate for abbreviations and source spans."""
        import re

        return [sentence for sentence in re.split(r"(?<=[.!?])\s+", content) if sentence.strip()]

    @step(input=paragraphs, output=sentences)
    def chunk(self, paragraph: Paragraph) -> Sentence:
        sentence_texts = arr_transform(
            self.default_sentence_texts(paragraph.content),
            lambda content: SentenceText(sentence_content=content),
        )
        sentence = posexplode_struct(
            sentence_texts,
            as_=ExpandedSentenceText,
            ordinal="position",
            scope="sentence_text",
        )
        content = trim(sentence.sentence_content)
        where(content != "")
        return Sentence(
            id=concat_ws("#s", paragraph.id, sentence.position.cast(types.string())),
            document_id=paragraph.document_id,
            section_id=paragraph.section_id,
            paragraph_id=paragraph.id,
            paragraph_ordinal=paragraph.ordinal,
            ordinal=(sentence.position + 1).cast(types.integer()),
            content=content,
            search_query_id=None,
            score_overlap=None,
            score_bm25=None,
        )
