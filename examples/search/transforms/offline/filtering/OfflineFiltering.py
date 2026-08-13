"""Offline filtering composition for target-scoped score preparation."""

from examples.search.schemas.filtering import DocumentFilterScore
from examples.search.schemas.indexing.lexical.index import DocumentTerm
from examples.search.schemas.search import DocumentSearchTarget, ScorePolicy, SearchQuery
from examples.search.transforms.filtering.Filtering import Filtering
from examples.search.transforms.offline.filtering.SelectOfflineFilterTargets import SelectOfflineFilterTargets
from structure import Transform, input, output


class OfflineFiltering(Transform):
    """Build reusable filter artifacts and their bounded scoring targets."""

    queries = input(SearchQuery)
    document_terms = input(DocumentTerm)
    score_policy = input(ScorePolicy)

    filtering = Filtering(
        queries=queries,
        document_terms=document_terms,
        score_policy=score_policy,
    )
    selected = SelectOfflineFilterTargets(document_filter_scores=filtering.document_filter_scores)

    document_filter_scores = output(DocumentFilterScore, filtering.document_filter_scores)
    targets = output(DocumentSearchTarget, selected.targets)
