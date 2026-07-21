"""Reusable, batch-built text index artifacts."""

from examples.texts.algorithms.scoring.TextIndex import TextIndex
from examples.texts.schemas.search import (
    DocumentIndexSummary,
    DocumentIndexTerm,
    ParagraphIndexSummary,
    ParagraphIndexTerm,
    SectionIndexSummary,
    SectionIndexTerm,
    SentenceIndexSummary,
    SentenceIndexTerm,
)
from examples.texts.schemas.text import Word
from structure import Transform, input, output, raw


class Index(Transform):
    """Build reusable document, section, paragraph, and sentence indexes."""

    words = input(Word)
    document_terms = output(DocumentIndexTerm)
    document_summary = output(DocumentIndexSummary)
    section_terms = output(SectionIndexTerm)
    section_summary = output(SectionIndexSummary)
    paragraph_terms = output(ParagraphIndexTerm)
    paragraph_summary = output(ParagraphIndexSummary)
    sentence_terms = output(SentenceIndexTerm)
    sentence_summary = output(SentenceIndexSummary)

    def declare_index(self, word: Word) -> tuple[
        DocumentIndexTerm,
        DocumentIndexSummary,
        SectionIndexTerm,
        SectionIndexSummary,
        ParagraphIndexTerm,
        ParagraphIndexSummary,
        SentenceIndexTerm,
        SentenceIndexSummary,
    ]:
        return (
            DocumentIndexTerm(
                document_id=word.document_id,
                token=word.token,
                term_frequency=0,
                target_word_count=0,
                target_distinct_terms=0,
                document_frequency=0,
            ),
            DocumentIndexSummary(target_count=0, average_target_length=0.0),
            SectionIndexTerm(
                document_id=word.document_id,
                section_id=word.section_id,
                token=word.token,
                term_frequency=0,
                target_word_count=0,
                target_distinct_terms=0,
                document_frequency=0,
            ),
            SectionIndexSummary(target_count=0, average_target_length=0.0),
            ParagraphIndexTerm(
                document_id=word.document_id,
                section_id=word.section_id,
                paragraph_id=word.paragraph_id,
                token=word.token,
                term_frequency=0,
                target_word_count=0,
                target_distinct_terms=0,
                document_frequency=0,
            ),
            ParagraphIndexSummary(target_count=0, average_target_length=0.0),
            SentenceIndexTerm(
                document_id=word.document_id,
                section_id=word.section_id,
                paragraph_id=word.paragraph_id,
                sentence_id=word.sentence_id,
                token=word.token,
                term_frequency=0,
                target_word_count=0,
                target_distinct_terms=0,
                document_frequency=0,
            ),
            SentenceIndexSummary(target_count=0, average_target_length=0.0),
        )

    @raw(
        input=input(words),
        output=[
            output(document_terms),
            output(document_summary),
            output(section_terms),
            output(section_summary),
            output(paragraph_terms),
            output(paragraph_summary),
            output(sentence_terms),
            output(sentence_summary),
        ],
    )
    def build(
        self,
        *,
        words,
        document_terms,
        document_summary,
        section_terms,
        section_summary,
        paragraph_terms,
        paragraph_summary,
        sentence_terms,
        sentence_summary,
        spark,
        ctx,
    ):
        return TextIndex.build(
            words,
            (
                document_terms,
                document_summary,
                section_terms,
                section_summary,
                paragraph_terms,
                paragraph_summary,
                sentence_terms,
                sentence_summary,
            ),
        )
