"""Stable document and query feature contracts for Search."""

from structure import Schema
from structure.plugin.pyspark import boolean, integer, long, string


class DocumentFeatures(Schema):
    """Reusable lexical and metadata features for one corpus document."""

    document_id = string(nullable=False)
    collection_id = string(nullable=False)
    source = string(nullable=False)
    language = string(nullable=False)
    normalized_title = string(nullable=False)
    normalized_content = string(nullable=False)
    title_length = integer(nullable=False)
    content_length = integer(nullable=False)
    url_is_https = boolean(nullable=True)


class QueryFeatures(Schema):
    """Reusable lexical and caller-supplied features for one search query."""

    query_id = string(nullable=False)
    queryset = string(nullable=False)
    language = string(nullable=True)
    normalized_content = string(nullable=False)
    token_count = long(nullable=False)
    distinct_token_count = long(nullable=False)
    is_question = boolean(nullable=False)
    is_time_sensitive = boolean(nullable=False)


class QueryFeatureToken(Schema):
    """One normalized query token before feature aggregation."""

    query_id = string(nullable=False)
    token = string(nullable=False)


class ExpandedQueryFeatureToken(QueryFeatureToken):
    """One normalized query token with its original query-local ordinal."""

    ordinal = long(nullable=False)


class QueryTokenSummary(Schema):
    """Token counts for one query before publication as query features."""

    query_id = string(nullable=False)
    token_count = long(nullable=False)
    distinct_token_count = long(nullable=False)
