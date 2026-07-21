from examples.search.schemas.analytics import (
    DocumentFeatures,
    DocumentStatistics,
    ParagraphStatistics,
    SectionStatistics,
    SentenceStatistics,
    SimilarDocument,
)
from examples.search.schemas.text import Paragraph, Section, Word
from structure import *
from structure.plugin.pyspark import *


class AnalyzeText(Transform):
    """Typed local, corpus, and blocked near-duplicate text analytics."""

    words = input(Word)
    paragraphs = input(Paragraph)
    sections = input(Section)
    comparison_left = input(DocumentFeatures)
    comparison_right = input(DocumentFeatures)
    sentence_statistics = output(SentenceStatistics)
    paragraph_statistics = output(ParagraphStatistics)
    section_statistics = output(SectionStatistics)
    document_statistics = output(DocumentStatistics)
    similar_documents = output(SimilarDocument)

    @step(input=words, output=sentence_statistics)
    def sentence_stats(self, word: Word) -> SentenceStatistics:
        group_by(
            sentence_id=word.sentence_id,
            document_id=word.document_id,
            paragraph_id=word.paragraph_id,
            section_id=word.section_id,
            ordinal=word.ordinal,
        )
        words_in_sentence = count()
        return SentenceStatistics(
            sentence_id=word.sentence_id,
            document_id=word.document_id,
            paragraph_id=word.paragraph_id,
            section_id=word.section_id,
            ordinal=word.ordinal,
            word_count=words_in_sentence,
            distinct_words=count_distinct(word.token),
            average_word_length=avg(length(word.token)),
        )

    @step(input=words, output=paragraph_statistics)
    def paragraph_stats(self, word: Word) -> ParagraphStatistics:
        group_by(
            paragraph_id=word.paragraph_id,
            document_id=word.document_id,
            section_id=word.section_id,
        )
        words_in_paragraph = count()
        return ParagraphStatistics(
            paragraph_id=word.paragraph_id,
            document_id=word.document_id,
            section_id=word.section_id,
            ordinal=min(word.paragraph_ordinal),
            word_count=words_in_paragraph,
            sentence_count=count_distinct(word.sentence_id),
            average_word_length=avg(length(word.token)),
        )

    @step(input=[words, sections], output=section_statistics)
    def section_stats(self, word: Word, section: Section) -> SectionStatistics:
        inner_join(on=word.section_id == section.id)
        group_by(
            section_id=section.id,
            document_id=section.document_id,
            section_ordinal=section.ordinal,
            heading=section.heading,
        )
        return SectionStatistics(
            section_id=section.id,
            document_id=section.document_id,
            section_ordinal=section.ordinal,
            heading=section.heading,
            paragraph_count=count_distinct(word.paragraph_id),
            sentence_count=count_distinct(word.sentence_id),
            word_count=count(),
            average_word_length=avg(length(word.token)),
        )

    @step(input=words, output=document_statistics)
    def document_stats(self, word: Word) -> DocumentStatistics:
        group_by(document_id=word.document_id)
        return DocumentStatistics(
            document_id=word.document_id,
            section_count=count_distinct(word.section_id),
            paragraph_count=count_distinct(word.paragraph_id),
            sentence_count=count_distinct(word.sentence_id),
            word_count=count(),
            distinct_words=count_distinct(word.token),
            average_word_length=avg(length(word.token)),
        )

    @step(input=[comparison_left, comparison_right], output=similar_documents)
    def similar(self, left: DocumentFeatures, right: DocumentFeatures) -> SimilarDocument:
        inner_join(
            on=(left.source == right.source)
            & (left.language == right.language)
            & (left.title_prefix == right.title_prefix)
        )
        where(left.document_id < right.document_id)
        return SimilarDocument(
            left_document_id=left.document_id,
            right_document_id=right.document_id,
            source=left.source,
            language=left.language,
            title_prefix=left.title_prefix,
            title_distance=levenshtein(left.title, right.title),
            content_distance=levenshtein(left.normalized_content, right.normalized_content),
        )
