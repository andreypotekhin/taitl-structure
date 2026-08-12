"""Normalize provider embeddings keyed by source similarity documents."""

from examples.search.schemas.indexing.vector import *
from examples.search.schemas.similarity import *
from structure import *
from structure.plugin.pyspark import *


class VectorizeSimilarityDocumentQueries(Transform):
    """Bind provider-produced embeddings to source-document identity."""

    queries = input(SimilarityDocumentQuery)
    embeddings = input(SimilarityDocumentVectorEmbedding)
    vector_queries = output(DocumentVectorQuery)

    @step(input=[queries, embeddings], output=vector_queries)
    def bind_query(
        self, query: SimilarityDocumentQuery, embedding: SimilarityDocumentVectorEmbedding
    ) -> DocumentVectorQuery:
        inner_join(on=query.id == embedding.query_id)
        return DocumentVectorQuery.project(embedding)(
            query_id=query.id,
            query_document_id=query.id,
        )
