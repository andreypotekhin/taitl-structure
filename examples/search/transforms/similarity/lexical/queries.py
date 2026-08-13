"""Create tagged self-queries from reusable text indexes."""

from datetime import datetime, timezone

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
from examples.search.schemas.label import LabelMapEntry
from examples.search.schemas.search import SearchQuery
from examples.search.schemas.similarities.intermediate import (
    DocumentSimilarityQueryText,
    ParagraphSimilarityQueryText,
    SectionSimilarityQueryText,
    SentenceSimilarityQueryText,
)
from examples.search.schemas.similarity import (
    DocumentSimilarityQuery,
    ParagraphSimilarityQuery,
    SectionSimilarityQuery,
    SentenceSimilarityQuery,
    SimilarityPolicy,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    array,
    collect_list,
    concat_ws,
    cross_join,
    group_by,
    map_from_entries,
    param_join,
    require_all,
    size,
    types,
    union_all,
    where,
)


class CreateSimilarityQueries(Transform):
    """Make document-to-sentence self-queries with optional common-term pruning."""

    policy = input(SimilarityPolicy)
    document_terms = input(DocumentTerm)
    document_summary = input(DocumentIndexSummary)
    section_terms = input(SectionTerm)
    section_summary = input(SectionIndexSummary)
    paragraph_terms = input(ParagraphTerm)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_terms = input(SentenceTerm)
    sentence_summary = input(SentenceIndexSummary)
    valid_policy = lane(SimilarityPolicy)
    document_query_text = lane(DocumentSimilarityQueryText)
    section_query_text = lane(SectionSimilarityQueryText)
    paragraph_query_text = lane(ParagraphSimilarityQueryText)
    sentence_query_text = lane(SentenceSimilarityQueryText)
    document_search_queries = lane(SearchQuery)
    section_search_queries = lane(SearchQuery)
    paragraph_search_queries = lane(SearchQuery)
    sentence_search_queries = lane(SearchQuery)
    queries = output(SearchQuery)
    document_queries = output(DocumentSimilarityQuery)
    section_queries = output(SectionSimilarityQuery)
    paragraph_queries = output(ParagraphSimilarityQuery)
    sentence_queries = output(SentenceSimilarityQuery)

    @step(input=policy, output=valid_policy)
    def validate_policy(self, policy: SimilarityPolicy) -> SimilarityPolicy:
        validated = require_all(
            policy.max_document_frequency_ratio.is_null()
            | ((policy.max_document_frequency_ratio > 0.0) & (policy.max_document_frequency_ratio <= 1.0))
        )
        return SimilarityPolicy.project(validated)

    @step(input=[document_terms, document_summary, valid_policy], output=document_query_text)
    def build_document_queries(
        self, term: DocumentTerm, summary: DocumentIndexSummary, policy: SimilarityPolicy
    ) -> DocumentSimilarityQueryText:
        self._retain(policy, summary, term)
        query_id = concat_ws("", "document:", term.document_id)
        group_by(query_id=query_id, document_id=term.document_id)
        return DocumentSimilarityQueryText(
            query_id=query_id,
            document_id=term.document_id,
            content_tokens=collect_list(term.term, order_by=term.term),
        )

    @step(input=[section_terms, section_summary, valid_policy], output=section_query_text)
    def build_section_queries(
        self, term: SectionTerm, summary: SectionIndexSummary, policy: SimilarityPolicy
    ) -> SectionSimilarityQueryText:
        self._retain(policy, summary, term)
        query_id = concat_ws("", "section:", term.section_id)
        group_by(query_id=query_id, document_id=term.document_id, section_id=term.section_id)
        return SectionSimilarityQueryText(
            query_id=query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            content_tokens=collect_list(term.term, order_by=term.term),
        )

    @step(input=[paragraph_terms, paragraph_summary, valid_policy], output=paragraph_query_text)
    def build_paragraph_queries(
        self, term: ParagraphTerm, summary: ParagraphIndexSummary, policy: SimilarityPolicy
    ) -> ParagraphSimilarityQueryText:
        self._retain(policy, summary, term)
        query_id = concat_ws("", "paragraph:", term.paragraph_id)
        group_by(
            query_id=query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
        )
        return ParagraphSimilarityQueryText(
            query_id=query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            content_tokens=collect_list(term.term, order_by=term.term),
        )

    @step(input=[sentence_terms, sentence_summary, valid_policy], output=sentence_query_text)
    def build_sentence_queries(
        self, term: SentenceTerm, summary: SentenceIndexSummary, policy: SimilarityPolicy
    ) -> SentenceSimilarityQueryText:
        self._retain(policy, summary, term)
        query_id = concat_ws("", "sentence:", term.sentence_id)
        group_by(
            query_id=query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
        )
        return SentenceSimilarityQueryText(
            query_id=query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
            content_tokens=collect_list(term.term, order_by=term.term),
        )

    @step(input=document_query_text, output=document_search_queries)
    def publish_document_search_queries(self, query: DocumentSimilarityQueryText) -> SearchQuery:
        return self._search_query(query.query_id, query.content_tokens)

    @step(input=section_query_text, output=section_search_queries)
    def publish_section_search_queries(self, query: SectionSimilarityQueryText) -> SearchQuery:
        return self._search_query(query.query_id, query.content_tokens)

    @step(input=paragraph_query_text, output=paragraph_search_queries)
    def publish_paragraph_search_queries(self, query: ParagraphSimilarityQueryText) -> SearchQuery:
        return self._search_query(query.query_id, query.content_tokens)

    @step(input=sentence_query_text, output=sentence_search_queries)
    def publish_sentence_search_queries(self, query: SentenceSimilarityQueryText) -> SearchQuery:
        return self._search_query(query.query_id, query.content_tokens)

    @step(input=document_query_text, output=document_queries)
    def publish_document_query_targets(self, query: DocumentSimilarityQueryText) -> DocumentSimilarityQuery:
        return DocumentSimilarityQuery(query_id=query.query_id, document_id=query.document_id)

    @step(input=section_query_text, output=section_queries)
    def publish_section_query_targets(self, query: SectionSimilarityQueryText) -> SectionSimilarityQuery:
        return SectionSimilarityQuery(
            query_id=query.query_id,
            document_id=query.document_id,
            section_id=query.section_id,
        )

    @step(input=paragraph_query_text, output=paragraph_queries)
    def publish_paragraph_query_targets(self, query: ParagraphSimilarityQueryText) -> ParagraphSimilarityQuery:
        return ParagraphSimilarityQuery(
            query_id=query.query_id,
            document_id=query.document_id,
            section_id=query.section_id,
            paragraph_id=query.paragraph_id,
        )

    @step(input=sentence_query_text, output=sentence_queries)
    def publish_sentence_query_targets(self, query: SentenceSimilarityQueryText) -> SentenceSimilarityQuery:
        return SentenceSimilarityQuery(
            query_id=query.query_id,
            document_id=query.document_id,
            section_id=query.section_id,
            paragraph_id=query.paragraph_id,
            sentence_id=query.sentence_id,
        )

    @step(
        input=[document_search_queries, section_search_queries, paragraph_search_queries, sentence_search_queries],
        output=queries,
    )
    def merge_queries(
        self,
        document: SearchQuery,
        section: SearchQuery,
        paragraph: SearchQuery,
        sentence: SearchQuery,
    ) -> SearchQuery:
        merged = union_all(section)
        merged = union_all(paragraph)
        merged = union_all(sentence)
        return SearchQuery.project(merged)

    def _retain(
        self,
        policy: SimilarityPolicy,
        summary: DocumentIndexSummary | SectionIndexSummary | ParagraphIndexSummary | SentenceIndexSummary,
        term: DocumentTerm | SectionTerm | ParagraphTerm | SentenceTerm,
    ) -> None:
        param_join(policy)
        cross_join(summary, allow_cartesian=True)
        where(
            policy.max_document_frequency_ratio.is_null()
            | (term.target_frequency / summary.target_count <= policy.max_document_frequency_ratio)
        )

    def _search_query(self, query_id: object, tokens: object) -> SearchQuery:
        zero = (size(tokens) * 0).cast(types.long())
        return SearchQuery(
            id=query_id,
            queryset="synthetic",
            content=concat_ws(" ", tokens),
            requested_at=datetime(1970, 1, 1, tzinfo=timezone.utc),
            labels=map_from_entries(
                array(
                    LabelMapEntry(key="is_question", value=zero),
                    LabelMapEntry(key="is_time_sensitive", value=zero),
                )
            ),
            is_question=False,
            is_time_sensitive=False,
            language=None,
        )
