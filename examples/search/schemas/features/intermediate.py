"""Intermediate query feature rows."""

from structure import Schema
from structure.plugin.pyspark import long, string


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
