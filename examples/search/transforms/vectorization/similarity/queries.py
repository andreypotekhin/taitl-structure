"""Normalize provider embeddings keyed by source similarity search queries."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.similarity import *
from structure import *
from structure.plugin.pyspark import *


class VectorizeSimilarityQueries(Transform):
    """Bind provider-produced embeddings to source-query identity."""

    queries = input(SimilaritySearchQuery)
    embeddings = input(SimilarityQueryEmbedding)
    vector_queries = output(DocumentVectorQuery)

    @step(input=[queries, embeddings], output=vector_queries)
    def bind_query(
        self, query: SimilaritySearchQuery, embedding: SimilarityQueryEmbedding
    ) -> DocumentVectorQuery:
        inner_join(on=query.id == embedding.query_id)
        return DocumentVectorQuery.project(embedding)(
            query_id=query.id,
            query_document_id=query.id,
        )
