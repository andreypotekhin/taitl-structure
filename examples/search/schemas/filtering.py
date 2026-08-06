"""Schemas for reusable and online document filtering artifacts."""

from structure import Schema
from structure.plugin.pyspark import long, string, timestamp


class DocumentFilterMatch(Schema):
    """An unpersisted simple-overlap match for one query and document."""

    query_id = string(nullable=False)
    document_id = string(nullable=False)
    matched_terms = long(nullable=False)
    filter_rank = long(nullable=False)


class DocumentFilterScore(Schema):
    """A timestamped, reusable simple-overlap filter result."""

    query_id = string(nullable=False)
    document_id = string(nullable=False)
    scored_at = timestamp(nullable=False)
    matched_terms = long(nullable=False)
    filter_rank = long(nullable=False)


class FilterQueryAvailability(Schema):
    """Internal marker that a query has a usable persisted filter result."""

    query_id = string(nullable=False)
