"""Delegate field-search body queries to the field-specific document funnel."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.fields import FieldSearchDelegation, FieldSearchDocumentMatch, FieldSearchQuery
from examples.search.schemas.search import DocumentSearchTarget, SearchQuery
from structure import Transform, input, output, step
from structure.plugin.pyspark import concat_ws, inner_join, sha2, where


class BuildDelegations(Transform):
    """Create child document queries, requests, targets, and identity mappings."""

    queries = input(FieldSearchQuery, streaming=True)
    document_matches = input(FieldSearchDocumentMatch, streaming=True)
    requests = input(SearchRequest, streaming=True)
    delegations = output(FieldSearchDelegation)
    body_queries = output(SearchQuery)
    delegated_requests = output(SearchRequest)
    document_filter_targets = output(DocumentSearchTarget)

    @step(input=queries, output=delegations)
    def build_delegations(self, query: FieldSearchQuery) -> FieldSearchDelegation:
        delegated_query_id = sha2(
            concat_ws("\x1f", "field-search-content-v1", query.id, query.content),
            bits=256,
        )
        return FieldSearchDelegation(query_id=query.id, delegated_query_id=delegated_query_id)

    @step(input=[queries, delegations], output=body_queries)
    def build_body_queries(
        self, query: FieldSearchQuery, delegation: FieldSearchDelegation
    ) -> SearchQuery:
        inner_join(on=delegation.query_id == query.id)
        return SearchQuery(
            id=delegation.delegated_query_id,
            queryset=query.queryset,
            content=query.content,
            requested_at=query.requested_at,
            labels=query.labels,
            is_question=query.is_question,
            is_time_sensitive=query.is_time_sensitive,
            language=query.language,
        )

    @step(input=[queries, delegations, requests], output=delegated_requests)
    def build_delegated_requests(
        self,
        query: FieldSearchQuery,
        delegation: FieldSearchDelegation,
        request: SearchRequest,
    ) -> SearchRequest:
        inner_join(on=request.query_id == query.id)
        inner_join(on=delegation.query_id == query.id)
        return SearchRequest.project(request)(
            query_id=delegation.delegated_query_id,
            query=query.content,
        )

    @step(input=[document_matches, queries, delegations], output=document_filter_targets)
    def build_document_filter_targets(
        self,
        document: FieldSearchDocumentMatch,
        query: FieldSearchQuery,
        delegation: FieldSearchDelegation,
    ) -> DocumentSearchTarget:
        inner_join(on=query.id == document.query_id)
        inner_join(on=delegation.query_id == query.id)
        where((query.operator == "or") | (document.matched_clause_count == document.expected_clause_count))
        return DocumentSearchTarget(
            query_id=delegation.delegated_query_id,
            document_id=document.document_id,
            scope_id=sha2(
                concat_ws("\x1f", "field-search-targets-v1", delegation.delegated_query_id),
                bits=256,
            ),
        )
