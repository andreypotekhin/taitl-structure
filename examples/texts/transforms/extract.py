from examples.texts.schemas.text import Document, Paragraph, Section, Sentence, Word
from examples.texts.transforms.extraction import TextExtraction
from structure import *
from structure.plugin.pyspark import *


class ExtractText(Transform):
    """Expand caller-extracted text through a narrow Spark-native raw boundary."""

    documents = input(Document)
    sections = output(Section)
    paragraphs = output(Paragraph)
    sentences = output(Sentence)
    words = output(Word)

    def declare_hierarchy(self, row: Document) -> tuple[Section, Paragraph, Sentence, Word]:
        """Declare the four output contracts before the row-expansion hook replaces them."""

        section_id = concat_ws("#s", row.id, "0")
        paragraph_id = concat_ws("#p", row.id, "0")
        sentence_id = concat_ws("#s", paragraph_id, "0")
        return (
            Section(id=section_id, document_id=row.id, ordinal=0, heading=row.title),
            Paragraph(id=paragraph_id, document_id=row.id, section_id=section_id, ordinal=0, content=row.content),
            Sentence(
                id=sentence_id,
                document_id=row.id,
                section_id=section_id,
                paragraph_id=paragraph_id,
                paragraph_ordinal=0,
                ordinal=0,
                content=row.content,
            ),
            Word(
                id=concat_ws("#w", sentence_id, "0"),
                document_id=row.id,
                section_id=section_id,
                paragraph_id=paragraph_id,
                paragraph_ordinal=0,
                sentence_id=sentence_id,
                ordinal=0,
                token=row.title,
            ),
        )

    @raw(inout=input(documents) | [output(sections), output(paragraphs), output(sentences), output(words)])
    def extract(self, *, documents, sections, paragraphs, sentences, words, spark, ctx):
        return TextExtraction.hierarchy(
            documents,
            sections=sections,
            paragraphs=paragraphs,
            sentences=sentences,
            words=words,
        )
