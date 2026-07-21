"""Create tagged self-queries from reusable text indexes."""

from examples.texts.algorithms.similarity.SimilarityQueries import SimilarityQueries
from examples.texts.schemas.search import (
    DocumentIndexSummary,
    DocumentIndexTerm,
    ParagraphIndexSummary,
    ParagraphIndexTerm,
    SearchQuery,
    SectionIndexSummary,
    SectionIndexTerm,
    SentenceIndexSummary,
    SentenceIndexTerm,
)
from examples.texts.schemas.similarity import (
    DocumentSimilarityQuery,
    ParagraphSimilarityQuery,
    SectionSimilarityQuery,
    SentenceSimilarityQuery,
    SimilarityPolicy,
)
from structure import Transform, input, output, raw, step


class CreateSimilarityQueries(Transform):
    """Make document-to-sentence self-queries with optional common-term pruning."""

    policy = input(SimilarityPolicy)
    document_terms = input(DocumentIndexTerm)
    document_summary = input(DocumentIndexSummary)
    section_terms = input(SectionIndexTerm)
    section_summary = input(SectionIndexSummary)
    paragraph_terms = input(ParagraphIndexTerm)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_terms = input(SentenceIndexTerm)
    sentence_summary = input(SentenceIndexSummary)
    queries = output(SearchQuery)
    document_queries = output(DocumentSimilarityQuery)
    section_queries = output(SectionSimilarityQuery)
    paragraph_queries = output(ParagraphSimilarityQuery)
    sentence_queries = output(SentenceSimilarityQuery)

    @step(
        input=document_terms,
        output=[queries, document_queries, section_queries, paragraph_queries, sentence_queries],
    )
    def declare_queries(
        self, term: DocumentIndexTerm
    ) -> tuple[
        SearchQuery, DocumentSimilarityQuery, SectionSimilarityQuery, ParagraphSimilarityQuery, SentenceSimilarityQuery
    ]:
        return (
            SearchQuery(id="", content=""),
            DocumentSimilarityQuery(query_id="", document_id=term.document_id),
            SectionSimilarityQuery(query_id="", document_id=term.document_id, section_id=""),
            ParagraphSimilarityQuery(query_id="", document_id=term.document_id, section_id="", paragraph_id=""),
            SentenceSimilarityQuery(
                query_id="", document_id=term.document_id, section_id="", paragraph_id="", sentence_id=""
            ),
        )

    @raw(
        input=[
            input(policy),
            input(document_terms),
            input(document_summary),
            input(section_terms),
            input(section_summary),
            input(paragraph_terms),
            input(paragraph_summary),
            input(sentence_terms),
            input(sentence_summary),
        ],
        output=[
            output(queries),
            output(document_queries),
            output(section_queries),
            output(paragraph_queries),
            output(sentence_queries),
        ],
    )
    def build(
        self,
        *,
        policy,
        document_terms,
        document_summary,
        section_terms,
        section_summary,
        paragraph_terms,
        paragraph_summary,
        sentence_terms,
        sentence_summary,
        queries,
        document_queries,
        section_queries,
        paragraph_queries,
        sentence_queries,
        spark,
        ctx,
    ):
        return SimilarityQueries.build(
            (document_terms, section_terms, paragraph_terms, sentence_terms),
            (document_summary, section_summary, paragraph_summary, sentence_summary),
            policy,
            (queries, document_queries, section_queries, paragraph_queries, sentence_queries),
        )
