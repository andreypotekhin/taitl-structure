"""Reusable, batch-built text index artifacts."""

from examples.search.schemas.chunking.intermediate import MaterializedSentence
from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexSummary,
    DocumentTerm,
    ParagraphIndexSummary,
    ParagraphTerm,
    SectionIndexSummary,
    SectionTerm,
    SentenceIndexSummary,
    SentenceTerm,
)
from examples.search.schemas.indexing.lexical.intermediate import (
    DocumentIndexTargetStats,
    DocumentTermCount,
    ExpandedTermText,
    IndexTargetFrequency,
    LexicalOccurrence,
    ParagraphIndexTargetStats,
    ParagraphTermCount,
    SectionIndexTargetStats,
    SectionTermCount,
    SentenceIndexTargetStats,
    SentenceTermCount,
    TermText,
)
from examples.search.schemas.scoring.intermediate import QueryToken
from examples.search.schemas.text import Document, Sentence
from examples.search.transforms.lib.Text import Text
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    arr_transform,
    avg,
    count,
    count_distinct,
    group_by,
    inner_join,
    length,
    posexplode_struct,
    split,
    where,
)


class LexIndex(Transform):
    """Build reusable document, section, paragraph, and sentence term indexes."""

    documents = input(Document)
    sentences = input(Sentence)

    materialized_sentence = lane(MaterializedSentence)
    occurrences = lane(LexicalOccurrence)
    document_term_counts = lane(DocumentTermCount)
    document_target_stats = lane(DocumentIndexTargetStats)
    document_target_frequencies = lane(IndexTargetFrequency)
    section_term_counts = lane(SectionTermCount)
    section_target_stats = lane(SectionIndexTargetStats)
    section_target_frequencies = lane(IndexTargetFrequency)
    paragraph_term_counts = lane(ParagraphTermCount)
    paragraph_target_stats = lane(ParagraphIndexTargetStats)
    paragraph_target_frequencies = lane(IndexTargetFrequency)
    sentence_term_counts = lane(SentenceTermCount)
    sentence_target_stats = lane(SentenceIndexTargetStats)
    sentence_target_frequencies = lane(IndexTargetFrequency)

    document_terms = output(DocumentTerm)
    document_summary = output(DocumentIndexSummary)
    section_terms = output(SectionTerm)
    section_summary = output(SectionIndexSummary)
    paragraph_terms = output(ParagraphTerm)
    paragraph_summary = output(ParagraphIndexSummary)
    sentence_terms = output(SentenceTerm)
    sentence_summary = output(SentenceIndexSummary)

    @step(input=[documents, sentences], output=materialized_sentence)
    def materialize_sentence(self, document: Document, sentence: Sentence) -> MaterializedSentence:
        inner_join(on=document.id == sentence.document_id)
        return MaterializedSentence.project(sentence)(
            content=Text.span(document.content, sentence.span_start, sentence.span_end),
        )

    @step(input=materialized_sentence, output=occurrences)
    def tokenize(self, sentence: MaterializedSentence) -> LexicalOccurrence:
        terms = arr_transform(
            split(sentence.content, pattern=r"\s+"),
            lambda value: TermText(term=value),
        )
        expanded = posexplode_struct(terms, as_=ExpandedTermText, ordinal="position", scope="sentence_term")
        term = QueryToken.normalize(expanded.term)
        where(term != "")
        return LexicalOccurrence(
            document_id=sentence.document_id,
            section_id=sentence.section_id,
            paragraph_id=sentence.paragraph_id,
            sentence_id=sentence.id,
            term=term,
        )

    @step(input=occurrences, output=document_term_counts)
    def count_document_terms(self, occurrence: LexicalOccurrence) -> DocumentTermCount:
        group_by(document_id=occurrence.document_id, term=occurrence.term)
        return DocumentTermCount(
            document_id=occurrence.document_id,
            term=occurrence.term,
            term_frequency=count(),
        )

    @step(input=occurrences, output=document_target_stats)
    def summarize_documents(self, occurrence: LexicalOccurrence) -> DocumentIndexTargetStats:
        group_by(document_id=occurrence.document_id)
        return DocumentIndexTargetStats(
            document_id=occurrence.document_id,
            target_term_count=count(),
            target_distinct_term_count=count_distinct(occurrence.term),
            target_average_term_length=avg(length(occurrence.term)),
        )

    @step(input=document_term_counts, output=document_target_frequencies)
    def count_document_frequencies(self, term: DocumentTermCount) -> IndexTargetFrequency:
        group_by(term=term.term)
        return IndexTargetFrequency(term=term.term, target_frequency=count())

    @step(
        input=[document_term_counts, document_target_stats, document_target_frequencies],
        output=document_terms,
    )
    def build_document_terms(
        self, term: DocumentTermCount, stats: DocumentIndexTargetStats, frequency: IndexTargetFrequency
    ) -> DocumentTerm:
        inner_join(stats, on=stats.document_id == term.document_id)
        inner_join(frequency, on=frequency.term == term.term)
        return DocumentTerm(
            document_id=term.document_id,
            term=term.term,
            term_frequency=term.term_frequency,
            target_term_count=stats.target_term_count,
            target_distinct_term_count=stats.target_distinct_term_count,
            target_average_term_length=stats.target_average_term_length,
            target_frequency=frequency.target_frequency,
        )

    @step(input=document_target_stats, output=document_summary)
    def summarize_document_index(self, stats: DocumentIndexTargetStats) -> DocumentIndexSummary:
        return DocumentIndexSummary(
            target_count=count(),
            average_target_length=avg(stats.target_term_count),
        )

    @step(input=occurrences, output=section_term_counts)
    def count_section_terms(self, occurrence: LexicalOccurrence) -> SectionTermCount:
        group_by(document_id=occurrence.document_id, section_id=occurrence.section_id, term=occurrence.term)
        return SectionTermCount(
            document_id=occurrence.document_id,
            section_id=occurrence.section_id,
            term=occurrence.term,
            term_frequency=count(),
        )

    @step(input=occurrences, output=section_target_stats)
    def summarize_sections(self, occurrence: LexicalOccurrence) -> SectionIndexTargetStats:
        group_by(document_id=occurrence.document_id, section_id=occurrence.section_id)
        return SectionIndexTargetStats(
            document_id=occurrence.document_id,
            section_id=occurrence.section_id,
            target_term_count=count(),
            target_distinct_term_count=count_distinct(occurrence.term),
            target_average_term_length=avg(length(occurrence.term)),
        )

    @step(input=section_term_counts, output=section_target_frequencies)
    def count_section_frequencies(self, term: SectionTermCount) -> IndexTargetFrequency:
        group_by(term=term.term)
        return IndexTargetFrequency(term=term.term, target_frequency=count())

    @step(input=[section_term_counts, section_target_stats, section_target_frequencies], output=section_terms)
    def build_section_terms(
        self, term: SectionTermCount, stats: SectionIndexTargetStats, frequency: IndexTargetFrequency
    ) -> SectionTerm:
        inner_join(
            stats,
            on=(stats.document_id == term.document_id) & (stats.section_id == term.section_id),
        )
        inner_join(frequency, on=frequency.term == term.term)
        return SectionTerm(
            document_id=term.document_id,
            section_id=term.section_id,
            term=term.term,
            term_frequency=term.term_frequency,
            target_term_count=stats.target_term_count,
            target_distinct_term_count=stats.target_distinct_term_count,
            target_average_term_length=stats.target_average_term_length,
            target_frequency=frequency.target_frequency,
        )

    @step(input=section_target_stats, output=section_summary)
    def summarize_section_index(self, stats: SectionIndexTargetStats) -> SectionIndexSummary:
        return SectionIndexSummary(
            target_count=count(),
            average_target_length=avg(stats.target_term_count),
        )

    @step(input=occurrences, output=paragraph_term_counts)
    def count_paragraph_terms(self, occurrence: LexicalOccurrence) -> ParagraphTermCount:
        group_by(
            document_id=occurrence.document_id,
            section_id=occurrence.section_id,
            paragraph_id=occurrence.paragraph_id,
            term=occurrence.term,
        )
        return ParagraphTermCount(
            document_id=occurrence.document_id,
            section_id=occurrence.section_id,
            paragraph_id=occurrence.paragraph_id,
            term=occurrence.term,
            term_frequency=count(),
        )

    @step(input=occurrences, output=paragraph_target_stats)
    def summarize_paragraphs(self, occurrence: LexicalOccurrence) -> ParagraphIndexTargetStats:
        group_by(
            document_id=occurrence.document_id,
            section_id=occurrence.section_id,
            paragraph_id=occurrence.paragraph_id,
        )
        return ParagraphIndexTargetStats(
            document_id=occurrence.document_id,
            section_id=occurrence.section_id,
            paragraph_id=occurrence.paragraph_id,
            target_term_count=count(),
            target_distinct_term_count=count_distinct(occurrence.term),
            target_average_term_length=avg(length(occurrence.term)),
        )

    @step(input=paragraph_term_counts, output=paragraph_target_frequencies)
    def count_paragraph_frequencies(self, term: ParagraphTermCount) -> IndexTargetFrequency:
        group_by(term=term.term)
        return IndexTargetFrequency(term=term.term, target_frequency=count())

    @step(
        input=[paragraph_term_counts, paragraph_target_stats, paragraph_target_frequencies],
        output=paragraph_terms,
    )
    def build_paragraph_terms(
        self, term: ParagraphTermCount, stats: ParagraphIndexTargetStats, frequency: IndexTargetFrequency
    ) -> ParagraphTerm:
        inner_join(
            stats,
            on=(stats.document_id == term.document_id)
            & (stats.section_id == term.section_id)
            & (stats.paragraph_id == term.paragraph_id),
        )
        inner_join(frequency, on=frequency.term == term.term)
        return ParagraphTerm(
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            term=term.term,
            term_frequency=term.term_frequency,
            target_term_count=stats.target_term_count,
            target_distinct_term_count=stats.target_distinct_term_count,
            target_average_term_length=stats.target_average_term_length,
            target_frequency=frequency.target_frequency,
        )

    @step(input=paragraph_target_stats, output=paragraph_summary)
    def summarize_paragraph_index(self, stats: ParagraphIndexTargetStats) -> ParagraphIndexSummary:
        return ParagraphIndexSummary(
            target_count=count(),
            average_target_length=avg(stats.target_term_count),
        )

    @step(input=occurrences, output=sentence_term_counts)
    def count_sentence_terms(self, occurrence: LexicalOccurrence) -> SentenceTermCount:
        group_by(
            document_id=occurrence.document_id,
            section_id=occurrence.section_id,
            paragraph_id=occurrence.paragraph_id,
            sentence_id=occurrence.sentence_id,
            term=occurrence.term,
        )
        return SentenceTermCount(
            document_id=occurrence.document_id,
            section_id=occurrence.section_id,
            paragraph_id=occurrence.paragraph_id,
            sentence_id=occurrence.sentence_id,
            term=occurrence.term,
            term_frequency=count(),
        )

    @step(input=occurrences, output=sentence_target_stats)
    def summarize_sentences(self, occurrence: LexicalOccurrence) -> SentenceIndexTargetStats:
        group_by(
            document_id=occurrence.document_id,
            section_id=occurrence.section_id,
            paragraph_id=occurrence.paragraph_id,
            sentence_id=occurrence.sentence_id,
        )
        return SentenceIndexTargetStats(
            document_id=occurrence.document_id,
            section_id=occurrence.section_id,
            paragraph_id=occurrence.paragraph_id,
            sentence_id=occurrence.sentence_id,
            target_term_count=count(),
            target_distinct_term_count=count_distinct(occurrence.term),
            target_average_term_length=avg(length(occurrence.term)),
        )

    @step(input=sentence_term_counts, output=sentence_target_frequencies)
    def count_sentence_frequencies(self, term: SentenceTermCount) -> IndexTargetFrequency:
        group_by(term=term.term)
        return IndexTargetFrequency(term=term.term, target_frequency=count())

    @step(
        input=[sentence_term_counts, sentence_target_stats, sentence_target_frequencies],
        output=sentence_terms,
    )
    def build_sentence_terms(
        self, term: SentenceTermCount, stats: SentenceIndexTargetStats, frequency: IndexTargetFrequency
    ) -> SentenceTerm:
        inner_join(
            stats,
            on=(stats.document_id == term.document_id)
            & (stats.section_id == term.section_id)
            & (stats.paragraph_id == term.paragraph_id)
            & (stats.sentence_id == term.sentence_id),
        )
        inner_join(frequency, on=frequency.term == term.term)
        return SentenceTerm(
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
            term=term.term,
            term_frequency=term.term_frequency,
            target_term_count=stats.target_term_count,
            target_distinct_term_count=stats.target_distinct_term_count,
            target_average_term_length=stats.target_average_term_length,
            target_frequency=frequency.target_frequency,
        )

    @step(input=sentence_target_stats, output=sentence_summary)
    def summarize_sentence_index(self, stats: SentenceIndexTargetStats) -> SentenceIndexSummary:
        return SentenceIndexSummary(
            target_count=count(),
            average_target_length=avg(stats.target_term_count),
        )
