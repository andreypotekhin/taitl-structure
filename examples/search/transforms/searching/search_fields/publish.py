"""Publish metadata matches and delegated document results as field results."""

from examples.search.schemas.fields import (
    FieldSearchDelegation,
    FieldSearchDocumentMatch,
    FieldSearchQuery,
    FieldSearchResult,
)
from examples.search.schemas.search import DocumentSearchResult
from structure import Transform, input, output, step
from structure.plugin.pyspark import inner_join, literal, where


class PublishFieldSearchResults(Transform):
    """Wrap delegated document results with their parent field-query identity."""

    queries = input(FieldSearchQuery, streaming=True)
    document_matches = input(FieldSearchDocumentMatch, streaming=True)
    delegations = input(FieldSearchDelegation, streaming=True)
    document_results = input(DocumentSearchResult, streaming=True)
    results = output(FieldSearchResult)

    @staticmethod
    def _parent_result(result: DocumentSearchResult, query: FieldSearchQuery) -> DocumentSearchResult:
        return DocumentSearchResult.project(result)(search_query_id=query.id)

    @step(input=[document_matches, queries], output=results)
    def publish_metadata(self, document: FieldSearchDocumentMatch, query: FieldSearchQuery) -> FieldSearchResult:
        inner_join(on=query.id == document.query_id)
        where(query.requires_content == False)
        where((query.operator == "or") | (document.matched_clause_count == document.expected_clause_count))
        return FieldSearchResult(
            query_id=document.query_id,
            document_id=document.document_id,
            match_scope="metadata",
            document_result=literal(None),
        )

    @step(input=[queries, delegations, document_results], output=results)
    def publish_content(
        self,
        query: FieldSearchQuery,
        delegation: FieldSearchDelegation,
        result: DocumentSearchResult,
    ) -> FieldSearchResult:
        inner_join(on=delegation.query_id == query.id)
        inner_join(on=result.search_query_id == delegation.delegated_query_id)
        where(query.clause_count == 0)
        return FieldSearchResult(
            query_id=query.id,
            document_id=result.document_id,
            match_scope="content",
            document_result=self._parent_result(result, query),
        )

    @step(input=[document_matches, queries, delegations, document_results], output=results)
    def publish_mixed(
        self,
        document: FieldSearchDocumentMatch,
        query: FieldSearchQuery,
        delegation: FieldSearchDelegation,
        result: DocumentSearchResult,
    ) -> FieldSearchResult:
        inner_join(query, on=query.id == document.query_id)
        inner_join(delegation, on=delegation.query_id == query.id)
        inner_join(
            result,
            on=(result.search_query_id == delegation.delegated_query_id)
            & (result.document_id == document.document_id),
        )
        where(query.requires_content & (query.clause_count > 0))
        where((query.operator == "or") | (document.matched_clause_count == document.expected_clause_count))
        return FieldSearchResult(
            query_id=query.id,
            document_id=document.document_id,
            match_scope="metadata+content",
            document_result=self._parent_result(result, query),
        )
