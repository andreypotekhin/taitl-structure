"""BM25-first document candidate retrieval."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.search import DocumentScore, DocumentSearchCandidate, SearchQuery
from examples.search.schemas.text import Document
from examples.search.schemas.user import UserBand
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import coalesce, inner_join, left_join, lower, regexp_replace, row_number, trim, where
from structure.plugin.pyspark.dsl.expressions import literal


class RetrieveDocuments(Transform):
    """Select the fixed-size lexical candidate set for every query."""

    maximum_candidates = 100

    queries = input(SearchQuery)
    documents = input(Document)
    document_scores = input(DocumentScore)
    requests = input(SearchRequest)
    user_bands = input(UserBand)
    candidates = output(DocumentSearchCandidate)

    @step(input=[documents, document_scores, queries, requests, user_bands], output=candidates)
    def select_candidates(
        self,
        document: Document,
        score: DocumentScore,
        query: SearchQuery,
        request: SearchRequest,
        user_band: UserBand,
    ) -> DocumentSearchCandidate:
        """Deduplicate lexical work for every requested reusable user-band context."""

        inner_join(on=document.id == score.document_id)
        inner_join(on=query.id == score.query_id)
        inner_join(on=request.query_id == query.id)
        left_join(on=user_band.user_id == request.user_id)
        where(score.score.is_not_null())
        return DocumentSearchCandidate.base(score, document)(
            search_query_id=query.id,
            band_id=coalesce(user_band.band_id, literal(None)),
            query=lower(regexp_replace(trim(query.content), pattern=r"\s+", replacement=" ")),
            candidate_rank=row_number(
                partition_by=(query.id, user_band.band_id, score.experiment_id),
                order_by=(score.score.desc_nulls_last(), document.id.asc_nulls_first()),
            ),
            document_id=document.id,
            score_feedback=0.0,
            score_rank=0.0,
            score_weight=0.0,
            feedback_weight=0.0,
        )
