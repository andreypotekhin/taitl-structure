"""Select online queries without a compatible cached embedding."""

from examples.search.schemas.indexing.vector import SearchQueryVectorEmbedding
from examples.search.schemas.inference import InferencePolicy
from examples.search.schemas.search import SearchQuery
from structure import Transform, input, output, step
from structure.plugin.pyspark import drop_duplicates, left_join, param_join, where


class SelectQueryGaps(Transform):
    queries = input(SearchQuery, streaming=True)
    embeddings = input(SearchQueryVectorEmbedding, streaming=True)
    policy = input(InferencePolicy)
    gaps = output(SearchQuery)

    @step(input=[queries, embeddings, policy], output=gaps)
    def select(self, query: SearchQuery, embedding: SearchQueryVectorEmbedding, policy: InferencePolicy) -> SearchQuery:
        left_join(embedding, on=query.id == embedding.query_id)
        param_join(policy)
        where(
            embedding.query_id.is_null()
            | (embedding.model_id != policy.model_id)
            | (embedding.dimension != policy.dimension)
            | (embedding.content_revision != policy.content_revision)
            | (embedding.experiment_id != policy.experiment_id)
        )
        drop_duplicates(query.id)
        return SearchQuery.project(query)
