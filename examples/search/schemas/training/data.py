"""Offline training rows derived from stable Search evidence."""

from structure import Schema
from structure.plugin.pyspark import boolean, double, integer, long, string


class DocumentTrainingData(Schema):
    """One judged document candidate with reusable query and document features."""

    search_query_id = string(nullable=False)
    document_id = string(nullable=False)
    relevance_grade = long(nullable=False)
    lexical_score = double(nullable=False)
    query_token_count = long(nullable=False)
    query_distinct_token_count = long(nullable=False)
    document_content_length = integer(nullable=False)
    document_url_is_https = boolean(nullable=True)
