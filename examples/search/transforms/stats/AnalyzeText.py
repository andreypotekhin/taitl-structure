from examples.search.schemas.analytics import (
    DocumentProfile,
    DocumentStatistics,
    ParagraphStatistics,
    SectionStatistics,
    SentenceStatistics,
    SimilarDocument,
)
from examples.search.schemas.chunking.intermediate import MaterializedSection
from examples.search.schemas.indexing.lexical.index import DocumentTerm, ParagraphTerm, SectionTerm, SentenceTerm
from examples.search.schemas.indexing.lexical.intermediate import DocumentHierarchyCounts
from examples.search.schemas.text import Document, Paragraph, Section, Sentence
from examples.search.transforms.lib.Text import Text
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import coalesce, count_distinct, group_by, inner_join, levenshtein, max, when


class AnalyzeText(Transform):
    """Typed local, corpus, and blocked near-duplicate text analytics."""

    documents = input(Document)
    sentences = input(Sentence)
    paragraphs = input(Paragraph)
    sections = input(Section)
    materialized_section = lane(MaterializedSection)
    document_terms = input(DocumentTerm)
    section_terms = input(SectionTerm)
    paragraph_terms = input(ParagraphTerm)
    sentence_terms = input(SentenceTerm)
    document_hierarchy_counts = lane(DocumentHierarchyCounts)
    comparison_left = input(DocumentProfile)
    comparison_right = input(DocumentProfile)
    sentence_statistics = output(SentenceStatistics)
    paragraph_statistics = output(ParagraphStatistics)
    section_statistics = output(SectionStatistics)
    document_statistics = output(DocumentStatistics)
    similar_documents = output(SimilarDocument)

    @step(input=[documents, sections], output=materialized_section)
    def materialize_section(self, document: Document, section: Section) -> MaterializedSection:
        inner_join(on=document.id == section.document_id)
        heading = when(
            section.heading_span_start.is_not_null(),
            Text.span(document.content, section.heading_span_start, section.heading_span_end),
        ).otherwise("Document")
        return MaterializedSection.project(section)(
            heading=coalesce(heading, "Document"),
        )

    @step(input=[sentence_terms, sentences], output=sentence_statistics)
    def sentence_stats(self, term: SentenceTerm, sentence: Sentence) -> SentenceStatistics:
        inner_join(on=term.sentence_id == sentence.id)
        group_by(
            sentence_id=sentence.id,
            document_id=sentence.document_id,
            paragraph_id=sentence.paragraph_id,
            section_id=sentence.section_id,
            ordinal=sentence.ordinal,
        )
        term_count = max(term.target_term_count)
        return SentenceStatistics.project(sentence)(
            sentence_id=sentence.id,
            word_count=term_count,
            distinct_words=max(term.target_distinct_term_count),
            average_word_length=max(term.target_average_term_length),
        )

    @step(input=[paragraph_terms, paragraphs, sentences], output=paragraph_statistics)
    def paragraph_stats(self, term: ParagraphTerm, paragraph: Paragraph, sentence: Sentence) -> ParagraphStatistics:
        inner_join(
            on=(term.document_id == paragraph.document_id)
            & (term.section_id == paragraph.section_id)
            & (term.paragraph_id == paragraph.id)
        )
        inner_join(
            on=(sentence.document_id == paragraph.document_id)
            & (sentence.section_id == paragraph.section_id)
            & (sentence.paragraph_id == paragraph.id)
        )
        group_by(
            paragraph_id=paragraph.id,
            document_id=paragraph.document_id,
            section_id=paragraph.section_id,
            ordinal=paragraph.ordinal,
        )
        term_count = max(term.target_term_count)
        return ParagraphStatistics.project(paragraph)(
            paragraph_id=paragraph.id,
            word_count=term_count,
            sentence_count=count_distinct(sentence.id),
            average_word_length=max(term.target_average_term_length),
        )

    @step(
        input=[section_terms, sections, paragraphs, sentences, materialized_section],
        output=section_statistics,
    )
    def section_stats(
        self,
        term: SectionTerm,
        section: Section,
        paragraph: Paragraph,
        sentence: Sentence,
        materialized_section: MaterializedSection,
    ) -> SectionStatistics:
        inner_join(on=(term.document_id == section.document_id) & (term.section_id == section.id))
        inner_join(on=(paragraph.document_id == section.document_id) & (paragraph.section_id == section.id))
        inner_join(on=(sentence.document_id == section.document_id) & (sentence.section_id == section.id))
        inner_join(on=materialized_section.id == section.id)
        group_by(
            section_id=section.id,
            document_id=section.document_id,
            section_ordinal=section.ordinal,
            heading=materialized_section.heading,
        )
        term_count = max(term.target_term_count)
        return SectionStatistics.project(section)(
            section_id=section.id,
            section_ordinal=section.ordinal,
            heading=materialized_section.heading,
            paragraph_count=count_distinct(paragraph.id),
            sentence_count=count_distinct(sentence.id),
            word_count=term_count,
            average_word_length=max(term.target_average_term_length),
        )

    @step(input=sentence_terms, output=document_hierarchy_counts)
    def document_hierarchy(self, term: SentenceTerm) -> DocumentHierarchyCounts:
        group_by(document_id=term.document_id)
        return DocumentHierarchyCounts.base(term)(
            section_count=count_distinct(term.section_id),
            paragraph_count=count_distinct(term.paragraph_id),
            sentence_count=count_distinct(term.sentence_id),
        )

    @step(input=[document_terms, document_hierarchy_counts], output=document_statistics)
    def document_stats(self, term: DocumentTerm, hierarchy: DocumentHierarchyCounts) -> DocumentStatistics:
        inner_join(on=term.document_id == hierarchy.document_id)
        group_by(document_id=term.document_id)
        term_count = max(term.target_term_count)
        return DocumentStatistics.project(term)(
            section_count=max(hierarchy.section_count),
            paragraph_count=max(hierarchy.paragraph_count),
            sentence_count=max(hierarchy.sentence_count),
            word_count=term_count,
            distinct_words=max(term.target_distinct_term_count),
            average_word_length=max(term.target_average_term_length),
        )

    @step(input=[comparison_left, comparison_right], output=similar_documents)
    def similar(self, left: DocumentProfile, right: DocumentProfile) -> SimilarDocument:
        inner_join(
            on=(left.source == right.source)
            & (left.language == right.language)
            & (left.title_prefix == right.title_prefix)
            & (left.document_id < right.document_id)
        )
        return SimilarDocument.project(left)(
            left_document_id=left.document_id,
            right_document_id=right.document_id,
            title_distance=levenshtein(left.title, right.title),
            content_distance=levenshtein(left.normalized_content, right.normalized_content),
        )
