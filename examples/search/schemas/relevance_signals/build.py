"""Intermediate schemas for building relevance signals."""

from examples.search.schemas.clicks import DailyClicks, DailyImpressions
from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals


class ContextDailyImpressions(DailyImpressions):
    """Internal impression fact projected into one feedback context."""


class ContextDailyClicks(DailyClicks):
    """Internal click fact projected into one feedback context."""


class QueryDocumentSignalTotals(QueryDocumentSignals):
    """Internal unnormalized query/document relevance totals."""


class DocumentPopularityTotals(DocumentPopularity):
    """Internal unnormalized document-popularity totals."""
