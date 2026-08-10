"""Target-aware replacement for canonical document search."""

from examples.search.schemas.search import DocumentSearchTarget
from examples.search.transforms.searching.search_docs.SearchDocuments import SearchDocuments as CanonicalSearchDocuments
from examples.search.transforms.searching.search_fields.online.filtering.OnlineFiltering import OnlineFiltering
from examples.search.transforms.searching.search_fields.search_docs.SelectFilterTargets import SelectFilterTargets
from structure import input


class SearchDocuments(CanonicalSearchDocuments):
    """Run the document funnel with field-projected filter targets."""

    document_filter_targets = input(DocumentSearchTarget, streaming=True)

    filtered = OnlineFiltering(
        queries=CanonicalSearchDocuments.queries,
        requests=CanonicalSearchDocuments.requests,
        document_filter_scores=CanonicalSearchDocuments.document_filter_scores,
        document_terms=CanonicalSearchDocuments.document_terms,
        document_filter_targets=document_filter_targets,
        score_policy=CanonicalSearchDocuments.score_policy,
    )

    selected = SelectFilterTargets(
        document_filter_scores=CanonicalSearchDocuments.document_filter_scores,
        online_document_filter_scores=filtered.online_document_filter_scores,
        document_filter_targets=document_filter_targets,
        requests=CanonicalSearchDocuments.requests,
        score_policy=CanonicalSearchDocuments.score_policy,
    )

