"""Fill missing or stale document-filter artifacts from reusable indexes."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.filtering import DocumentFilterScore
from examples.search.schemas.indexing.lexical.index import DocumentTerm
from examples.search.schemas.search import DocumentSearchTarget, ScorePolicy, SearchQuery
from examples.search.transforms.filtering.Filtering import Filtering
from examples.search.transforms.searching.online.filtering.SelectGapQueries import SelectGapQueries
from structure import Transform, input, output


class OnlineFiltering(Transform):
    """Calculate filter artifacts only for query groups missing from the cache."""

    queries = input(SearchQuery, streaming=True)
    requests = input(SearchRequest, streaming=True)
    document_filter_scores = input(DocumentFilterScore)
    document_terms = input(DocumentTerm)
    document_filter_targets = input(DocumentSearchTarget, streaming=True)
    score_policy = input(ScorePolicy)

    gap = SelectGapQueries(
        queries=queries,
        requests=requests,
        document_filter_scores=document_filter_scores,
        document_filter_targets=document_filter_targets,
        score_policy=score_policy,
    )

    filtering = Filtering(
        queries=gap.gap_queries,
        document_terms=document_terms,
        document_filter_targets=document_filter_targets,
        score_policy=score_policy,
    )

    online_document_filter_scores = output(DocumentFilterScore, filtering.document_filter_scores)


__all__ = ["OnlineFiltering"]
