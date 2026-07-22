from examples.search.schemas.analytics import (
    DocumentFeatures,
    DocumentStatistics,
    ParagraphStatistics,
    SectionStatistics,
    SentenceStatistics,
    SimilarDocument,
)
from examples.search.schemas.text import Paragraph, Section, Sentence, Word
from structure import *
from structure.plugin.pyspark import *


class AnalyzeText(Transform):
    """Typed local, corpus, and blocked near-duplicate text analytics."""

    words = input(Word)
    sentences = input(Sentence)
    paragraphs = input(Paragraph)
    sections = input(Section)
    comparison_left = input(DocumentFeatures)
    comparison_right = input(DocumentFeatures)
    sentence_statistics = output(SentenceStatistics)
    paragraph_statistics = output(ParagraphStatistics)
    section_statistics = output(SectionStatistics)
    document_statistics = output(DocumentStatistics)
    similar_documents = output(SimilarDocument)

    @step(input=[words, sentences], output=sentence_statistics)
    def sentence_stats(self, word: Word, sentence: Sentence) -> SentenceStatistics:
        inner_join(on=word.sentence_id == sentence.id)
        group_by(
            sentence_id=sentence.id,
            document_id=sentence.document_id,
            paragraph_id=sentence.paragraph_id,
            section_id=sentence.section_id,
            ordinal=sentence.ordinal,
        )
        words_in_sentence = count()
        return SentenceStatistics.base(sentence)(
            sentence_id=sentence.id,
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
        return ParagraphStatistics.base(word)(
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
        return SectionStatistics.base(section)(
            section_id=section.id,
            section_ordinal=section.ordinal,
            paragraph_count=count_distinct(word.paragraph_id),
            sentence_count=count_distinct(word.sentence_id),
            word_count=count(),
            average_word_length=avg(length(word.token)),
        )

    @step(input=words, output=document_statistics)
    def document_stats(self, word: Word) -> DocumentStatistics:
        group_by(document_id=word.document_id)
        return DocumentStatistics.base(word)(
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
        return SimilarDocument.base(left)(
            left_document_id=left.document_id,
            right_document_id=right.document_id,
            title_distance=levenshtein(left.title, right.title),
            content_distance=levenshtein(left.normalized_content, right.normalized_content),
        )
