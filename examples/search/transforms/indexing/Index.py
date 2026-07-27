"""Reusable, batch-built text index artifacts."""

from examples.search.schemas.search import (
    DocumentIndexSummary,
    DocumentIndexTargetStats,
    DocumentIndexTerm,
    DocumentIndexTermCount,
    IndexTokenFrequency,
    ParagraphIndexSummary,
    ParagraphIndexTargetStats,
    ParagraphIndexTerm,
    ParagraphIndexTermCount,
    SectionIndexSummary,
    SectionIndexTargetStats,
    SectionIndexTerm,
    SectionIndexTermCount,
    SentenceIndexSummary,
    SentenceIndexTargetStats,
    SentenceIndexTerm,
    SentenceIndexTermCount,
)
from examples.search.schemas.text import Word
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import avg, count, count_distinct, group_by, inner_join


class Index(Transform):
    """Build reusable document, section, paragraph, and sentence indexes."""

    words = input(Word)
    document_term_counts = lane(DocumentIndexTermCount)
    document_target_stats = lane(DocumentIndexTargetStats)
    document_token_frequencies = lane(IndexTokenFrequency)
    section_term_counts = lane(SectionIndexTermCount)
    section_target_stats = lane(SectionIndexTargetStats)
    section_token_frequencies = lane(IndexTokenFrequency)
    paragraph_term_counts = lane(ParagraphIndexTermCount)
    paragraph_target_stats = lane(ParagraphIndexTargetStats)
    paragraph_token_frequencies = lane(IndexTokenFrequency)
    sentence_term_counts = lane(SentenceIndexTermCount)
    sentence_target_stats = lane(SentenceIndexTargetStats)
    sentence_token_frequencies = lane(IndexTokenFrequency)
    document_terms = output(DocumentIndexTerm)
    document_summary = output(DocumentIndexSummary)
    section_terms = output(SectionIndexTerm)
    section_summary = output(SectionIndexSummary)
    paragraph_terms = output(ParagraphIndexTerm)
    paragraph_summary = output(ParagraphIndexSummary)
    sentence_terms = output(SentenceIndexTerm)
    sentence_summary = output(SentenceIndexSummary)

    @step(input=words, output=document_term_counts)
    def count_document_terms(self, word: Word) -> DocumentIndexTermCount:
        group_by(document_id=word.document_id, token=word.token)
        return DocumentIndexTermCount(
            document_id=word.document_id,
            token=word.token,
            term_frequency=count(),
        )

    @step(input=words, output=document_target_stats)
    def summarize_documents(self, word: Word) -> DocumentIndexTargetStats:
        group_by(document_id=word.document_id)
        return DocumentIndexTargetStats(
            document_id=word.document_id,
            target_word_count=count(),
            target_distinct_terms=count_distinct(word.token),
        )

    @step(input=document_term_counts, output=document_token_frequencies)
    def count_document_frequencies(self, term: DocumentIndexTermCount) -> IndexTokenFrequency:
        group_by(token=term.token)
        return IndexTokenFrequency(token=term.token, document_frequency=count())

    @step(input=[document_term_counts, document_target_stats, document_token_frequencies], output=document_terms)
    def build_document_terms(
        self, term: DocumentIndexTermCount, stats: DocumentIndexTargetStats, frequency: IndexTokenFrequency
    ) -> DocumentIndexTerm:
        inner_join(stats, on=stats.document_id == term.document_id)
        inner_join(frequency, on=frequency.token == term.token)
        return DocumentIndexTerm(
            document_id=term.document_id,
            token=term.token,
            term_frequency=term.term_frequency,
            target_word_count=stats.target_word_count,
            target_distinct_terms=stats.target_distinct_terms,
            document_frequency=frequency.document_frequency,
        )

    @step(input=document_target_stats, output=document_summary)
    def summarize_document_index(self, stats: DocumentIndexTargetStats) -> DocumentIndexSummary:
        return DocumentIndexSummary(
            target_count=count(),
            average_target_length=avg(stats.target_word_count),
        )

    @step(input=words, output=section_term_counts)
    def count_section_terms(self, word: Word) -> SectionIndexTermCount:
        group_by(document_id=word.document_id, section_id=word.section_id, token=word.token)
        return SectionIndexTermCount(
            document_id=word.document_id,
            section_id=word.section_id,
            token=word.token,
            term_frequency=count(),
        )

    @step(input=words, output=section_target_stats)
    def summarize_sections(self, word: Word) -> SectionIndexTargetStats:
        group_by(document_id=word.document_id, section_id=word.section_id)
        return SectionIndexTargetStats(
            document_id=word.document_id,
            section_id=word.section_id,
            target_word_count=count(),
            target_distinct_terms=count_distinct(word.token),
        )

    @step(input=section_term_counts, output=section_token_frequencies)
    def count_section_frequencies(self, term: SectionIndexTermCount) -> IndexTokenFrequency:
        group_by(token=term.token)
        return IndexTokenFrequency(token=term.token, document_frequency=count())

    @step(input=[section_term_counts, section_target_stats, section_token_frequencies], output=section_terms)
    def build_section_terms(
        self, term: SectionIndexTermCount, stats: SectionIndexTargetStats, frequency: IndexTokenFrequency
    ) -> SectionIndexTerm:
        inner_join(stats, on=(stats.document_id == term.document_id) & (stats.section_id == term.section_id))
        inner_join(frequency, on=frequency.token == term.token)
        return SectionIndexTerm(
            document_id=term.document_id,
            section_id=term.section_id,
            token=term.token,
            term_frequency=term.term_frequency,
            target_word_count=stats.target_word_count,
            target_distinct_terms=stats.target_distinct_terms,
            document_frequency=frequency.document_frequency,
        )

    @step(input=section_target_stats, output=section_summary)
    def summarize_section_index(self, stats: SectionIndexTargetStats) -> SectionIndexSummary:
        return SectionIndexSummary(
            target_count=count(),
            average_target_length=avg(stats.target_word_count),
        )

    @step(input=words, output=paragraph_term_counts)
    def count_paragraph_terms(self, word: Word) -> ParagraphIndexTermCount:
        group_by(
            document_id=word.document_id,
            section_id=word.section_id,
            paragraph_id=word.paragraph_id,
            token=word.token,
        )
        return ParagraphIndexTermCount(
            document_id=word.document_id,
            section_id=word.section_id,
            paragraph_id=word.paragraph_id,
            token=word.token,
            term_frequency=count(),
        )

    @step(input=words, output=paragraph_target_stats)
    def summarize_paragraphs(self, word: Word) -> ParagraphIndexTargetStats:
        group_by(document_id=word.document_id, section_id=word.section_id, paragraph_id=word.paragraph_id)
        return ParagraphIndexTargetStats(
            document_id=word.document_id,
            section_id=word.section_id,
            paragraph_id=word.paragraph_id,
            target_word_count=count(),
            target_distinct_terms=count_distinct(word.token),
        )

    @step(input=paragraph_term_counts, output=paragraph_token_frequencies)
    def count_paragraph_frequencies(self, term: ParagraphIndexTermCount) -> IndexTokenFrequency:
        group_by(token=term.token)
        return IndexTokenFrequency(token=term.token, document_frequency=count())

    @step(input=[paragraph_term_counts, paragraph_target_stats, paragraph_token_frequencies], output=paragraph_terms)
    def build_paragraph_terms(
        self, term: ParagraphIndexTermCount, stats: ParagraphIndexTargetStats, frequency: IndexTokenFrequency
    ) -> ParagraphIndexTerm:
        inner_join(
            stats,
            on=(stats.document_id == term.document_id)
            & (stats.section_id == term.section_id)
            & (stats.paragraph_id == term.paragraph_id),
        )
        inner_join(frequency, on=frequency.token == term.token)
        return ParagraphIndexTerm(
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            token=term.token,
            term_frequency=term.term_frequency,
            target_word_count=stats.target_word_count,
            target_distinct_terms=stats.target_distinct_terms,
            document_frequency=frequency.document_frequency,
        )

    @step(input=paragraph_target_stats, output=paragraph_summary)
    def summarize_paragraph_index(self, stats: ParagraphIndexTargetStats) -> ParagraphIndexSummary:
        return ParagraphIndexSummary(
            target_count=count(),
            average_target_length=avg(stats.target_word_count),
        )

    @step(input=words, output=sentence_term_counts)
    def count_sentence_terms(self, word: Word) -> SentenceIndexTermCount:
        group_by(
            document_id=word.document_id,
            section_id=word.section_id,
            paragraph_id=word.paragraph_id,
            sentence_id=word.sentence_id,
            token=word.token,
        )
        return SentenceIndexTermCount(
            document_id=word.document_id,
            section_id=word.section_id,
            paragraph_id=word.paragraph_id,
            sentence_id=word.sentence_id,
            token=word.token,
            term_frequency=count(),
        )

    @step(input=words, output=sentence_target_stats)
    def summarize_sentences(self, word: Word) -> SentenceIndexTargetStats:
        group_by(
            document_id=word.document_id,
            section_id=word.section_id,
            paragraph_id=word.paragraph_id,
            sentence_id=word.sentence_id,
        )
        return SentenceIndexTargetStats(
            document_id=word.document_id,
            section_id=word.section_id,
            paragraph_id=word.paragraph_id,
            sentence_id=word.sentence_id,
            target_word_count=count(),
            target_distinct_terms=count_distinct(word.token),
        )

    @step(input=sentence_term_counts, output=sentence_token_frequencies)
    def count_sentence_frequencies(self, term: SentenceIndexTermCount) -> IndexTokenFrequency:
        group_by(token=term.token)
        return IndexTokenFrequency(token=term.token, document_frequency=count())

    @step(input=[sentence_term_counts, sentence_target_stats, sentence_token_frequencies], output=sentence_terms)
    def build_sentence_terms(
        self, term: SentenceIndexTermCount, stats: SentenceIndexTargetStats, frequency: IndexTokenFrequency
    ) -> SentenceIndexTerm:
        inner_join(
            stats,
            on=(stats.document_id == term.document_id)
            & (stats.section_id == term.section_id)
            & (stats.paragraph_id == term.paragraph_id)
            & (stats.sentence_id == term.sentence_id),
        )
        inner_join(frequency, on=frequency.token == term.token)
        return SentenceIndexTerm(
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
            token=term.token,
            term_frequency=term.term_frequency,
            target_word_count=stats.target_word_count,
            target_distinct_terms=stats.target_distinct_terms,
            document_frequency=frequency.document_frequency,
        )

    @step(input=sentence_target_stats, output=sentence_summary)
    def summarize_sentence_index(self, stats: SentenceIndexTargetStats) -> SentenceIndexSummary:
        return SentenceIndexSummary(
            target_count=count(),
            average_target_length=avg(stats.target_word_count),
        )
