"""Normalize provider embeddings keyed by request-time SearchQuery rows."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.search import *
from structure import *
from structure.plugin.pyspark import *
from structure.plugin.pyspark.dsl.expressions import literal


class VectorizeSearchQueries(Transform):
    """Bind provider-produced query embeddings to SearchQuery identity."""

    queries = input(SearchQuery, streaming=True)
    embeddings = input(SearchQueryVectorEmbedding, streaming=True)
    vector_queries = output(DocumentVectorQuery)

    @step(input=[queries, embeddings], output=vector_queries)
    def bind_query(self, query: SearchQuery, embedding: SearchQueryVectorEmbedding) -> DocumentVectorQuery:
        inner_join(on=query.id == embedding.query_id)
        return DocumentVectorQuery.project(embedding)(query_document_id=literal(None))
