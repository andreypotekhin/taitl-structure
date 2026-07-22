"""BM25-first document candidate retrieval."""

from examples.search.schemas.search import DocumentSearchCandidate, SearchQuery
from examples.search.schemas.text import Document
from structure import Transform, input, lane, step
from structure.plugin.pyspark import inner_join, lower, regexp_replace, row_number, trim, where


class RetrieveDocuments(Transform):
    """Select the fixed-size lexical candidate set for every query."""

    maximum_candidates = 100

    queries = input(SearchQuery)
    scored_documents = input(Document)
    candidates = lane(DocumentSearchCandidate)

    @step(input=[scored_documents, queries], output=candidates)
    def select_candidates(self, document: Document, query: SearchQuery) -> DocumentSearchCandidate:
        query = inner_join(query, on=query.id == document.search_query_id)
        where(document.score_bm25.is_not_null())
        return DocumentSearchCandidate.base(document)(
            search_query_id=query.id,
            query=lower(regexp_replace(trim(query.content), pattern=r"\s+", replacement=" ")),
            candidate_rank=row_number(
                partition_by=query.id,
                order_by=(document.score_bm25.desc_nulls_last(), document.id.asc_nulls_first()),
            ),
            document_id=document.id,
            score_feedback=0.0,
            score_rank=0.0,
            bm25_weight=0.0,
            feedback_weight=0.0,
        )
