"""Scoring-internal intermediate schemas."""

from examples.search.algorithms.text import normalized_token
from examples.search.schemas.search import (
    DocumentSearchTarget,
    ParagraphSearchTarget,
    SearchQuery,
    SectionSearchTarget,
    SentenceSearchTarget,
)
from structure import Schema
from structure.plugin.pyspark import arr_distinct, arr_transform, double, long, posexplode_struct, split, string, trim


class QueryToken(Schema):
    """One normalized query token before row expansion."""

    token = string(nullable=False)

    @staticmethod
    def expand(query: SearchQuery):
        """Expand normalized, distinct query tokens into rows."""

        return posexplode_struct(
            arr_transform(
                arr_distinct(split(trim(query.content), pattern=r"\s+")),
                lambda value: QueryToken(token=normalized_token(value)),
            ),
            as_=ExpandedQueryToken,
            scope="query_token",
        )


class ExpandedQueryToken(Schema):
    """One expanded query token with its original query-local ordinal."""

    ordinal = long(nullable=False)
    token = string(nullable=False)


class QueryTerm(Schema):
    """One distinct normalized query term."""

    query_id = string(nullable=False)
    token = string(nullable=False)


class QueryTermCount(Schema):
    """Number of distinct normalized terms in one query."""

    query_id = string(nullable=False)
    query_terms = long(nullable=False)


class ScoreQueryAvailability(Schema):
    """One query with a fresh score relation available for serving."""

    query_id = string(nullable=False)


class PopularQueryCandidate(SearchQuery):
    """Offline query row ranked by observed impression volume."""

    impression_count = long(nullable=False)
    popularity_rank = long(nullable=False)


class DocumentOverlapMatch(DocumentSearchTarget):
    """Aggregate IDF-weighted overlap fields for one document."""

    query_idf = double(nullable=False)
    matched_idf = double(nullable=False)


class SectionOverlapMatch(DocumentOverlapMatch, SectionSearchTarget):
    """Aggregate overlap fields for one section."""


class ParagraphOverlapMatch(SectionOverlapMatch, ParagraphSearchTarget):
    """Aggregate overlap fields for one paragraph."""


class SentenceOverlapMatch(ParagraphOverlapMatch, SentenceSearchTarget):
    """Aggregate overlap fields for one sentence."""


class QueryTermIdf(Schema):
    """IDF weight for one distinct query term at one target grain."""

    query_id = string(nullable=False)
    token = string(nullable=False)
    idf = double(nullable=False)


class QueryIdfTotal(Schema):
    """Total possible IDF for one normalized query at one target grain."""

    query_id = string(nullable=False)
    query_idf = double(nullable=False)
